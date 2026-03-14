import 'dart:async';

import 'saga.dart';
import 'behavior_context.dart';
import 'event_handler.dart';
import 'activity.dart';
import 'scheduler.dart';
import 'saga_repository.dart';

/// Callback for state transitions.
typedef TransitionCallback<TSaga extends Saga, TState> = void Function(
  TSaga saga,
  TState from,
  TState to,
);

/// Callback for saga updates (property changes without state transition).
typedef UpdateCallback<TSaga extends Saga> = void Function(TSaga saga);

/// Callback for saga finalization.
typedef FinalizeCallback<TSaga extends Saga> = void Function(TSaga saga);

/// Event correlator function.
typedef EventCorrelator<TEvent> = String Function(TEvent event);

/// MassTransit-style state machine for sagas.
///
/// Provides a declarative, fluent API for defining state transitions:
///
/// ```dart
/// class OrderStateMachine extends SagaStateMachine<OrderSaga, OrderStatus> {
///   OrderStateMachine() {
///     initially(
///       when<OrderCreated>()
///         .set((saga, e) => saga.orderId = e.id)
///         .transitionTo(OrderStatus.pending),
///     );
///
///     during(OrderStatus.pending,
///       when<PaymentReceived>().transitionTo(OrderStatus.paid),
///       timeout(Duration(hours: 24), transitionTo: OrderStatus.expired),
///     );
///   }
/// }
/// ```
abstract class SagaStateMachine<TSaga extends Saga, TState> {
  final Map<Type, Function> _correlators = {};
  final List<_EventHandlerEntry<TSaga, TState>> _initialHandlers = [];
  final Map<TState, List<_EventHandlerEntry<TSaga, TState>>> _stateHandlers = {};
  final List<_EventHandlerEntry<TSaga, TState>> _anyStateHandlers = [];
  final Map<TState, _TimeoutMarker<TSaga, TState>> _timeouts = {};
  final List<Activity<TSaga, void>> _finalizeActivities = [];

  TransitionCallback<TSaga, TState>? _onTransition;
  UpdateCallback<TSaga>? _onUpdate;
  FinalizeCallback<TSaga>? _onFinalize;

  late final Scheduler _scheduler;
  late SagaRepository<TSaga> _repository;

  /// Create a new state machine.
  SagaStateMachine() {
    _scheduler = Scheduler(_onScheduledEvent);
    _repository = InMemorySagaRepository<TSaga>();
    correlate<_StateTimeoutEvent<TState>>((e) => e.sagaId);
  }

  // ─────────────────────────────────────────────────────────────────
  // ABSTRACT METHODS - Must be implemented by subclass
  // ─────────────────────────────────────────────────────────────────

  /// Create a new saga instance with the given correlation ID.
  TSaga createSaga(String correlationId);

  /// Get the current state of the saga.
  TState getState(TSaga saga);

  /// Set the state of the saga.
  void setState(TSaga saga, TState state);

  // ─────────────────────────────────────────────────────────────────
  // CONFIGURATION API
  // ─────────────────────────────────────────────────────────────────

  /// Set a custom repository.
  void useRepository(SagaRepository<TSaga> repository) {
    _repository = repository;
  }

  // ─────────────────────────────────────────────────────────────────
  // CONFIGURATION API
  // ─────────────────────────────────────────────────────────────────

  /// Define event correlation.
  ///
  /// ```dart
  /// correlate<OrderCreated>((e) => e.orderId);
  /// ```
  void correlate<TEvent>(EventCorrelator<TEvent> correlator) {
    _correlators[TEvent] = correlator;
  }

  /// Define initial state handlers (for new saga creation).
  ///
  /// ```dart
  /// initially(
  ///   when<OrderCreated>().transitionTo(OrderStatus.pending),
  /// );
  /// ```
  void initially(EventHandler<TSaga, dynamic, TState> handler, [List<EventHandler<TSaga, dynamic, TState>>? more]) {
    _initialHandlers.add(_EventHandlerEntry(handler));
    if (more != null) {
      for (final h in more) {
        _initialHandlers.add(_EventHandlerEntry(h));
      }
    }
  }

  /// Define handlers for a specific state.
  ///
  /// ```dart
  /// during(OrderStatus.pending,
  ///   when<PaymentReceived>().transitionTo(OrderStatus.paid),
  ///   when<OrderCancelled>().transitionTo(OrderStatus.cancelled).finalize(),
  /// );
  /// ```
  void during(TState state, EventHandler<TSaga, dynamic, TState> handler,
      [List<EventHandler<TSaga, dynamic, TState>>? more]) {
    _stateHandlers.putIfAbsent(state, () => []);
    _stateHandlers[state]!.add(_EventHandlerEntry(handler));
    if (handler is _TimeoutMarker<TSaga, TState>) {
      _timeouts[state] = handler;
    }
    if (more != null) {
      for (final h in more) {
        _stateHandlers[state]!.add(_EventHandlerEntry(h));
        if (h is _TimeoutMarker<TSaga, TState>) {
          _timeouts[state] = h;
        }
      }
    }
  }

  /// Define handlers that apply to any state.
  ///
  /// ```dart
  /// duringAny(
  ///   when<ForceCancel>().transitionTo(OrderStatus.cancelled).finalize(),
  /// );
  /// ```
  void duringAny(EventHandler<TSaga, dynamic, TState> handler, [List<EventHandler<TSaga, dynamic, TState>>? more]) {
    _anyStateHandlers.add(_EventHandlerEntry(handler));
    if (more != null) {
      for (final h in more) {
        _anyStateHandlers.add(_EventHandlerEntry(h));
      }
    }
  }

  /// Define a timeout for a state.
  ///
  /// ```dart
  /// during(OrderStatus.pending,
  ///   timeout(Duration(hours: 24), transitionTo: OrderStatus.expired),
  /// );
  /// ```
  EventHandler<TSaga, dynamic, TState> timeout(
    Duration duration, {
    required TState transitionTo,
    bool finalize = false,
    List<Activity<TSaga, dynamic>>? activities,
  }) {
    // Return a dummy handler - timeout is handled separately
    return _TimeoutMarker<TSaga, TState>(
      duration: duration,
      targetState: transitionTo,
      shouldFinalize: finalize,
      activities: activities ?? [],
    );
  }

  /// Define activities to run when saga is finalized.
  ///
  /// ```dart
  /// whenFinalized(
  ///   execute: CleanupActivity(),
  /// );
  /// ```
  void whenFinalized({Activity<TSaga, void>? execute, List<Activity<TSaga, void>>? activities}) {
    if (execute != null) _finalizeActivities.add(execute);
    if (activities != null) _finalizeActivities.addAll(activities);
  }

  /// Register callback for any state transition.
  void onAnyTransition(TransitionCallback<TSaga, TState> callback) {
    _onTransition = callback;
  }

  /// Register callback for saga updates (property changes without state transition).
  /// This is called when saga properties are modified but no state transition occurs.
  void onSagaUpdated(UpdateCallback<TSaga> callback) {
    _onUpdate = callback;
  }

  /// Register callback for saga finalization.
  void onSagaFinalized(FinalizeCallback<TSaga> callback) {
    _onFinalize = callback;
  }

  // ─────────────────────────────────────────────────────────────────
  // EVENT HANDLER BUILDER
  // ─────────────────────────────────────────────────────────────────

  /// Create an event handler for a specific event type.
  ///
  /// ```dart
  /// when<OrderCreated>()
  ///   .set((saga, e) => saga.orderId = e.id)
  ///   .transitionTo(OrderStatus.pending)
  /// ```
  EventHandler<TSaga, TEvent, TState> when<TEvent>() {
    return EventHandler<TSaga, TEvent, TState>();
  }

  // ─────────────────────────────────────────────────────────────────
  // RUNTIME API
  // ─────────────────────────────────────────────────────────────────

  /// Dispatch an event to be handled by the state machine.
  ///
  /// Returns the saga instance if one was found or created.
  Future<TSaga?> dispatch<TEvent>(TEvent event) async {
    final correlationId = _getCorrelationId(event);
    if (correlationId == null) {
      throw StateError('No correlator registered for event type ${event.runtimeType}');
    }

    var saga = _repository.getById(correlationId);
    final isNew = saga == null;

    // Try initial handlers for new saga
    if (isNew) {
      for (final entry in _initialHandlers) {
        if (entry.canHandle(event) && entry.handler.matches(event as dynamic)) {
          saga = createSaga(correlationId);
          await _executeHandler(saga, event, entry.handler, null);
          _repository.save(saga);
          return saga;
        }
      }
      return null; // No matching initial handler
    }

    // Get current state
    final currentState = getState(saga);

    // Try state-specific handlers
    final handlers = _stateHandlers[currentState] ?? [];
    for (final entry in handlers) {
      if (entry.canHandle(event) && entry.handler.matches(event as dynamic)) {
        await _executeHandler(saga, event, entry.handler, currentState);
        _repository.save(saga);
        if (saga.isFinalized) {
          _repository.remove(saga.id);
        }
        return saga;
      }
    }

    // Try any-state handlers
    for (final entry in _anyStateHandlers) {
      if (entry.canHandle(event) && entry.handler.matches(event as dynamic)) {
        await _executeHandler(saga, event, entry.handler, currentState);
        _repository.save(saga);
        if (saga.isFinalized) {
          _repository.remove(saga.id);
        }
        return saga;
      }
    }

    return saga; // No matching handler
  }

  /// Execute a handler on a saga.
  Future<void> _executeHandler(
    TSaga saga,
    dynamic event,
    EventHandler<TSaga, dynamic, TState> handler,
    TState? currentState,
  ) async {
    // Unschedule events
    for (final _ in handler.unscheduleTypes) {
      _scheduler.unschedule(saga.id);
    }

    // Execute setters, activities, and actions with proper type safety
    await handler.executeWith(saga, event, _scheduler, currentState);

    // Handle transition
    final targetState = handler.getTargetStateWith(saga, event, _scheduler, currentState);
    if (targetState != null && targetState != currentState) {
      // For initial transitions, use the saga's initial state as "from"
      final fromState = currentState ?? getState(saga);
      setState(saga, targetState);
      saga.markUpdated();
      _onTransition?.call(saga, fromState, targetState);

      // Set up timeout for new state if defined
      _setupStateTimeout(saga, targetState);
    } else {
      // No state transition, but saga may have been updated (e.g., mute toggle)
      // Notify listeners if the handler had setters, activities, or actions that could modify the saga
      final hasModifiers = handler.hasModifiers;
      if (hasModifiers) {
        saga.markUpdated();
        _onUpdate?.call(saga);
      }
    }

    // Schedule events
    if (handler.scheduleTimeout != null && handler.scheduleEventType != null) {
      _scheduleEvent(saga.id, handler.scheduleEventType!, handler.scheduleTimeout!);
    }

    // Handle finalization
    if (handler.shouldFinalize) {
      await _finalizeSaga(saga);
    }
  }

  /// Handle a scheduled event.
  void _onScheduledEvent<TEvent>(String sagaId, TEvent event) {
    dispatch(event);
  }

  /// Set up timeout for a state.
  void _setupStateTimeout(TSaga saga, TState state) {
    final timeout = _timeouts[state];
    if (timeout != null) {
      // Schedule timeout event
      _scheduler.schedule(
        sagaId: saga.id,
        event: _StateTimeoutEvent(saga.id, state),
        delay: timeout.duration,
      );
    }
  }

  /// Schedule an event.
  void _scheduleEvent(String sagaId, Type eventType, Duration delay) {
    // This is handled by the specific event scheduling in handlers
  }

  /// Finalize a saga.
  Future<void> _finalizeSaga(TSaga saga) async {
    _scheduler.unscheduleAll(saga.id);

    final context = BehaviorContext<TSaga, void>(
      saga: saga,
      event: null,
      scheduler: _scheduler,
    );

    for (final activity in _finalizeActivities) {
      await activity.execute(context);
    }

    saga.markFinalized();
    _onFinalize?.call(saga);
  }

  /// Get correlation ID for an event.
  String? _getCorrelationId<TEvent>(TEvent event) {
    final correlator = _correlators[event.runtimeType];
    if (correlator != null) {
      return correlator(event);
    }

    // Try to find by assignability
    for (final entry in _correlators.entries) {
      if (event.runtimeType == entry.key) {
        return entry.value(event);
      }
    }

    return null;
  }

  // ─────────────────────────────────────────────────────────────────
  // UTILITY
  // ─────────────────────────────────────────────────────────────────

  /// Get a saga by ID.
  TSaga? getSaga(String id) => _repository.getById(id);

  /// Get all active sagas.
  List<TSaga> getActiveSagas() => _repository.getActive();

  /// Dispose the state machine.
  void dispose() {
    _scheduler.dispose();
    _repository.clear();
  }
}

/// Internal entry for event handlers.
class _EventHandlerEntry<TSaga extends Saga, TState> {
  final EventHandler<TSaga, dynamic, TState> handler;

  _EventHandlerEntry(this.handler);

  /// Check if this entry can handle the event.
  bool canHandle(dynamic event) => handler.canHandle(event);
}

/// Marker class for timeout configuration.
class _TimeoutMarker<TSaga extends Saga, TState> extends EventHandler<TSaga, dynamic, TState> {
  final Duration duration;

  _TimeoutMarker({
    required this.duration,
    required super.targetState,
    required super.shouldFinalize,
    required super.activities,
  });

  @override
  bool canHandle(dynamic event) => event is _StateTimeoutEvent<TState>;
}

/// Internal timeout event.
class _StateTimeoutEvent<TState> {
  final String sagaId;
  final TState state;

  _StateTimeoutEvent(this.sagaId, this.state);
}
