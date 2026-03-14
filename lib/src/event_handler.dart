import 'dart:async';

import 'saga.dart';
import 'behavior_context.dart';
import 'activity.dart';
import 'scheduler.dart';

/// Callback types for saga operations.

/// Function that sets saga properties from an event.
typedef SagaSetter<TSaga extends Saga, TEvent> = void Function(TSaga saga, TEvent event);

/// Async action executed during event handling.
typedef SagaAction<TSaga extends Saga, TEvent> = FutureOr<void> Function(BehaviorContext<TSaga, TEvent> context);

/// Predicate function to filter events.
typedef EventFilter<TEvent> = bool Function(TEvent event);

/// Function that dynamically resolves the target state.
typedef StateResolver<TSaga extends Saga, TEvent, TState> = TState Function(BehaviorContext<TSaga, TEvent> context);

/// Defines how to handle a specific event type.
///
/// Built using a fluent API:
/// ```dart
/// when<OrderCreated>()
///   .where((e) => e.amount > 100)
///   .set((saga, e) => saga.orderId = e.id)
///   .execute(NotifyActivity())
///   .transitionTo(OrderStatus.created)
/// ```
class EventHandler<TSaga extends Saga, TEvent, TState> {
  final EventFilter<TEvent>? _filter;
  final List<SagaSetter<TSaga, TEvent>> _setters = [];
  final List<Activity<TSaga, TEvent>> _activities;
  final List<SagaAction<TSaga, TEvent>> _actions = [];
  final List<Type> _unscheduleTypes = [];
  TState? _targetState;
  StateResolver<TSaga, TEvent, TState>? _stateResolver;
  bool _shouldFinalize = false;
  Duration? _scheduleTimeout;
  Type? _scheduleEventType;

  /// Creates a new event handler.
  ///
  /// Optionally provide a [filter] to only handle matching events.
  EventHandler({
    EventFilter<TEvent>? filter,
    TState? targetState,
    bool shouldFinalize = false,
    List<Activity<TSaga, TEvent>>? activities,
  }) :
    _filter = filter,
    _targetState = targetState,
    _shouldFinalize = shouldFinalize,
    _activities = activities ?? [];

  /// The event type this handler handles.
  Type get eventType => TEvent;

  /// Check if this handler can handle the given event.
  bool canHandle(dynamic event) => event is TEvent;

  /// Filter events by a condition.
  EventHandler<TSaga, TEvent, TState> where(EventFilter<TEvent> filter) {
    return EventHandler<TSaga, TEvent, TState>(
      filter: _filter != null ? (e) => _filter!(e) && filter(e) : filter,
    )
      .._setters.addAll(_setters)
      .._activities.addAll(_activities)
      .._actions.addAll(_actions)
      .._unscheduleTypes.addAll(_unscheduleTypes)
      .._targetState = _targetState
      .._stateResolver = _stateResolver
      .._shouldFinalize = _shouldFinalize
      .._scheduleTimeout = _scheduleTimeout
      .._scheduleEventType = _scheduleEventType;
  }

  /// Set saga properties from the event.
  EventHandler<TSaga, TEvent, TState> set(SagaSetter<TSaga, TEvent> setter) {
    _setters.add(setter);
    return this;
  }

  /// Execute a custom action.
  EventHandler<TSaga, TEvent, TState> then(SagaAction<TSaga, TEvent> action) {
    _actions.add(action);
    return this;
  }

  /// Execute an activity (with optional compensation).
  EventHandler<TSaga, TEvent, TState> execute(Activity<TSaga, TEvent> activity) {
    _activities.add(activity);
    return this;
  }

  /// Transition to a specific state.
  EventHandler<TSaga, TEvent, TState> transitionTo(TState state) {
    _targetState = state;
    return this;
  }

  /// Transition to a dynamically determined state.
  EventHandler<TSaga, TEvent, TState> transitionToState(StateResolver<TSaga, TEvent, TState> resolver) {
    _stateResolver = resolver;
    return this;
  }

  /// Mark the saga as finalized after this handler.
  EventHandler<TSaga, TEvent, TState> finalize() {
    _shouldFinalize = true;
    return this;
  }

  /// Schedule a timeout event.
  EventHandler<TSaga, TEvent, TState> schedule<TTimeoutEvent>(Duration timeout) {
    _scheduleTimeout = timeout;
    _scheduleEventType = TTimeoutEvent;
    return this;
  }

  /// Cancel a previously scheduled event.
  EventHandler<TSaga, TEvent, TState> unschedule<TScheduledEvent>() {
    _unscheduleTypes.add(TScheduledEvent);
    return this;
  }

  /// Check if this handler matches the event.
  bool matches(TEvent event) => _filter == null || _filter!(event);

  /// Get the target state for transition.
  TState? getTargetState(BehaviorContext<TSaga, TEvent> context) {
    if (_stateResolver != null) return _stateResolver!(context);
    return _targetState;
  }

  /// Execute all setters, activities, and actions with the given saga and event.
  /// This method maintains type safety by executing with the proper TEvent type.
  Future<void> executeWith(TSaga saga, dynamic event, Scheduler scheduler, dynamic previousState) async {
    final typedEvent = event as TEvent;

    // Apply setters
    for (final setter in _setters) {
      setter(saga, typedEvent);
    }

    final context = BehaviorContext<TSaga, TEvent>(
      saga: saga,
      event: typedEvent,
      scheduler: scheduler,
      previousState: previousState,
    );

    // Execute activities
    for (final activity in _activities) {
      await activity.execute(context);
    }

    // Execute actions
    for (final action in _actions) {
      await action(context);
    }
  }

  /// Get the target state for transition (uses dynamic event cast internally).
  TState? getTargetStateWith(TSaga saga, dynamic event, Scheduler scheduler, dynamic previousState) {
    final typedEvent = event as TEvent;
    final context = BehaviorContext<TSaga, TEvent>(
      saga: saga,
      event: typedEvent,
      scheduler: scheduler,
      previousState: previousState,
    );
    if (_stateResolver != null) return _stateResolver!(context);
    return _targetState;
  }

  /// Get all setters.
  List<SagaSetter<TSaga, TEvent>> get setters => List.unmodifiable(_setters);

  /// Get all activities.
  List<Activity<TSaga, TEvent>> get activities => List.unmodifiable(_activities);

  /// Get all actions.
  List<SagaAction<TSaga, TEvent>> get actions => List.unmodifiable(_actions);

  /// Whether this handler has any setters, activities, or actions that modify the saga.
  bool get hasModifiers => _setters.isNotEmpty || _activities.isNotEmpty || _actions.isNotEmpty;

  /// Get types to unschedule.
  List<Type> get unscheduleTypes => List.unmodifiable(_unscheduleTypes);

  /// Whether this handler finalizes the saga.
  bool get shouldFinalize => _shouldFinalize;

  /// Timeout to schedule (if any).
  Duration? get scheduleTimeout => _scheduleTimeout;

  /// Event type to schedule (if any).
  Type? get scheduleEventType => _scheduleEventType;
}

/// Timeout handler configuration.
class TimeoutHandler<TSaga extends Saga, TState> {
  final Duration duration;
  final TState targetState;
  final bool shouldFinalize;
  final List<Activity<TSaga, void>> activities;

  TimeoutHandler({
    required this.duration,
    required this.targetState,
    this.shouldFinalize = false,
    this.activities = const [],
  });
}
