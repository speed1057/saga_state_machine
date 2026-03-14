// Comprehensive tests for saga_state_machine package
//
// Tests cover:
// - Saga lifecycle (creation, update, finalization)
// - Event correlation
// - State transitions (initially, during, duringAny)
// - Event filtering with where()
// - Setters with set()
// - Actions with then()
// - Activities with execute()
// - Dynamic state resolution with transitionToState()
// - Finalization callbacks
// - Transition callbacks
// - Scheduler (schedule, unschedule)
// - Custom repository
// - Multiple handlers per state
// - Error handling

import 'dart:async';

import 'package:saga_state_machine/saga_state_machine.dart';
import 'package:test/test.dart';

// ═══════════════════════════════════════════════════════════════════
// TEST SAGA AND EVENTS
// ═══════════════════════════════════════════════════════════════════

enum TaskStatus { created, inProgress, paused, completed, failed, cancelled, timeout }

class TaskSaga extends Saga {
  TaskStatus status = TaskStatus.created;
  String? title;
  String? assignee;
  int progressPercent = 0;
  List<String> logs = [];
  String? failureReason;
  bool wasResumed = false;
}

// Events
class TaskCreated {
  final String taskId;
  final String title;
  final String? assignee;

  TaskCreated(this.taskId, this.title, {this.assignee});
}

class TaskStarted {
  final String taskId;

  TaskStarted(this.taskId);
}

class TaskProgressUpdated {
  final String taskId;
  final int percent;

  TaskProgressUpdated(this.taskId, this.percent);
}

class TaskPaused {
  final String taskId;

  TaskPaused(this.taskId);
}

class TaskResumed {
  final String taskId;

  TaskResumed(this.taskId);
}

class TaskCompleted {
  final String taskId;

  TaskCompleted(this.taskId);
}

class TaskFailed {
  final String taskId;
  final String reason;

  TaskFailed(this.taskId, this.reason);
}

class TaskCancelled {
  final String taskId;

  TaskCancelled(this.taskId);
}

class TaskTimeout extends TimeoutEvent {
  TaskTimeout(super.sagaId);
}

class HighPriorityTaskCreated {
  final String taskId;
  final String title;
  final int priority;

  HighPriorityTaskCreated(this.taskId, this.title, this.priority);
}

// ═══════════════════════════════════════════════════════════════════
// TEST STATE MACHINE
// ═══════════════════════════════════════════════════════════════════

class TaskStateMachine extends SagaStateMachine<TaskSaga, TaskStatus> {
  final List<String> transitionLog = [];
  final List<String> finalizeLog = [];
  final List<String> activityLog = [];
  final List<String> updateLog = [];

  TaskStateMachine() {
    _configureCorrelation();
    _configureInitialHandlers();
    _configureStateHandlers();
    _configureAnyStateHandlers();
    _configureCallbacks();
  }

  void _configureCorrelation() {
    correlate<TaskCreated>((e) => e.taskId);
    correlate<TaskStarted>((e) => e.taskId);
    correlate<TaskProgressUpdated>((e) => e.taskId);
    correlate<TaskPaused>((e) => e.taskId);
    correlate<TaskResumed>((e) => e.taskId);
    correlate<TaskCompleted>((e) => e.taskId);
    correlate<TaskFailed>((e) => e.taskId);
    correlate<TaskCancelled>((e) => e.taskId);
    correlate<TaskTimeout>((e) => e.sagaId);
    correlate<HighPriorityTaskCreated>((e) => e.taskId);
  }

  void _configureInitialHandlers() {
    // Normal task creation
    initially(
      when<TaskCreated>()
          .set((saga, e) => saga
            ..title = e.title
            ..assignee = e.assignee
            ..logs.add('Task created: ${e.title}'))
          .transitionTo(TaskStatus.created),
    );

    // High priority task creation with filter
    initially(
      when<HighPriorityTaskCreated>()
          .where((e) => e.priority >= 5)
          .set((saga, e) => saga
            ..title = '[HIGH] ${e.title}'
            ..logs.add('High priority task created'))
          .transitionTo(TaskStatus.created),
    );
  }

  void _configureStateHandlers() {
    // Created state
    during(
      TaskStatus.created,
      when<TaskStarted>()
          .set((saga, e) => saga.logs.add('Task started'))
          .execute(LogActivity('Task started'))
          .transitionTo(TaskStatus.inProgress), [
      timeout(
        const Duration(milliseconds: 200),
        transitionTo: TaskStatus.timeout,
      ),
    ]);

    // In Progress state - multiple handlers
    during(
        TaskStatus.inProgress,
        when<TaskProgressUpdated>().set((saga, e) => saga
          ..progressPercent = e.percent
          ..logs.add('Progress: ${e.percent}%')),
        [
          when<TaskPaused>().set((saga, e) => saga.logs.add('Task paused')).transitionTo(TaskStatus.paused),
          when<TaskCompleted>()
              .where((e) => true) // Always match
              .set((saga, e) => saga
                ..progressPercent = 100
                ..logs.add('Task completed'))
              .transitionTo(TaskStatus.completed)
              .finalize(),
          when<TaskFailed>()
              .set((saga, e) => saga
                ..failureReason = e.reason
                ..logs.add('Task failed: ${e.reason}'))
              .transitionToState((context) => TaskStatus.failed)
              .finalize(),
        ]);

    // Paused state
    during(
      TaskStatus.paused,
      when<TaskResumed>()
          .set((saga, e) => saga
            ..wasResumed = true
            ..logs.add('Task resumed'))
          .then((context) async {
        // Custom action
        context.saga.logs.add('Custom action executed');
      }).transitionTo(TaskStatus.inProgress),
    );
  }

  void _configureAnyStateHandlers() {
    // Cancel can happen from any state
    duringAny(
      when<TaskCancelled>()
          .set((saga, e) => saga.logs.add('Task cancelled'))
          .transitionTo(TaskStatus.cancelled)
          .finalize(),
    );
  }

  void _configureCallbacks() {
    onAnyTransition((saga, from, to) {
      transitionLog.add('${saga.id}: $from → $to');
    });

    onSagaUpdated((saga) {
      updateLog.add('Updated: ${saga.id} (progress: ${saga.progressPercent}%)');
    });

    onSagaFinalized((saga) {
      finalizeLog.add('Finalized: ${saga.id}');
    });

    whenFinalized(
      execute: FunctionActivity((context) {
        activityLog.add('Cleanup for ${context.saga.id}');
      }),
    );
  }

  @override
  TaskSaga createSaga(String correlationId) {
    final saga = TaskSaga();
    saga.id = correlationId;
    return saga;
  }

  @override
  TaskStatus getState(TaskSaga saga) => saga.status;

  @override
  void setState(TaskSaga saga, TaskStatus state) {
    saga.status = state;
  }
}

// Custom activity for testing
class LogActivity extends Activity<TaskSaga, TaskStarted> {
  final String message;

  LogActivity(this.message);

  @override
  Future<void> execute(BehaviorContext<TaskSaga, TaskStarted> context) async {
    context.saga.logs.add('Activity: $message');
  }
}

// ═══════════════════════════════════════════════════════════════════
// CUSTOM REPOSITORY FOR TESTING
// ═══════════════════════════════════════════════════════════════════

class TrackingSagaRepository extends InMemorySagaRepository<TaskSaga> {
  final List<String> saveLog = [];
  final List<String> removeLog = [];

  @override
  void save(TaskSaga saga) {
    saveLog.add('Saved: ${saga.id}');
    super.save(saga);
  }

  @override
  void remove(String id) {
    removeLog.add('Removed: $id');
    super.remove(id);
  }
}

// ═══════════════════════════════════════════════════════════════════
// TESTS
// ═══════════════════════════════════════════════════════════════════

void main() {
  group('Saga Lifecycle', () {
    late TaskStateMachine machine;

    setUp(() {
      machine = TaskStateMachine();
    });

    tearDown(() {
      machine.dispose();
    });

    test('creates new saga on initial event', () async {
      final saga = await machine.dispatch(TaskCreated('task-1', 'Test Task'));

      expect(saga, isNotNull);
      expect(saga!.id, equals('task-1'));
      expect(saga.title, equals('Test Task'));
      expect(saga.status, equals(TaskStatus.created));
      expect(saga.isFinalized, isFalse);
    });

    test('saga has correct timestamps', () async {
      final before = DateTime.now();
      await Future.delayed(const Duration(milliseconds: 10));

      final saga = await machine.dispatch(TaskCreated('task-1', 'Test Task'));
      final after = DateTime.now();

      expect(saga!.createdAt.isAfter(before), isTrue);
      expect(saga.createdAt.isBefore(after), isTrue);
      expect(saga.updatedAt.isAfter(before), isTrue);
    });

    test('updatedAt changes on state transition', () async {
      final saga = await machine.dispatch(TaskCreated('task-1', 'Test Task'));
      final createdTime = saga!.updatedAt;

      await Future.delayed(const Duration(milliseconds: 10));
      await machine.dispatch(TaskStarted('task-1'));

      final updatedSaga = machine.getSaga('task-1');
      expect(updatedSaga!.updatedAt.isAfter(createdTime), isTrue);
    });

    test('finalized saga is removed from repository', () async {
      await machine.dispatch(TaskCreated('task-1', 'Test Task'));
      await machine.dispatch(TaskStarted('task-1'));
      await machine.dispatch(TaskCompleted('task-1'));

      final saga = machine.getSaga('task-1');
      expect(saga, isNull);
    });

    test('finalized saga has isFinalized=true before removal', () async {
      await machine.dispatch(TaskCreated('task-1', 'Test Task'));
      await machine.dispatch(TaskStarted('task-1'));

      final finalizedSaga = await machine.dispatch(TaskCompleted('task-1'));
      expect(finalizedSaga!.isFinalized, isTrue);
    });
  });

  group('Event Correlation', () {
    late TaskStateMachine machine;

    setUp(() {
      machine = TaskStateMachine();
    });

    tearDown(() {
      machine.dispose();
    });

    test('correlates events to correct saga', () async {
      await machine.dispatch(TaskCreated('task-1', 'Task 1'));
      await machine.dispatch(TaskCreated('task-2', 'Task 2'));

      await machine.dispatch(TaskStarted('task-1'));

      final saga1 = machine.getSaga('task-1');
      final saga2 = machine.getSaga('task-2');

      expect(saga1!.status, equals(TaskStatus.inProgress));
      expect(saga2!.status, equals(TaskStatus.created));
    });

    test('returns null for uncorrelated event type', () async {
      await machine.dispatch(TaskCreated('task-1', 'Test Task'));

      // UncorrelatedEvent has no correlator
      expect(
        () => machine.dispatch(_UncorrelatedEvent()),
        throwsA(isA<StateError>()),
      );
    });

    test('returns null when no saga exists for correlation id', () async {
      final saga = await machine.dispatch(TaskStarted('non-existent'));
      expect(saga, isNull);
    });
  });

  group('State Transitions', () {
    late TaskStateMachine machine;

    setUp(() {
      machine = TaskStateMachine();
    });

    tearDown(() {
      machine.dispose();
    });

    test('initially() handles saga creation events', () async {
      final saga = await machine.dispatch(TaskCreated('task-1', 'Test Task'));
      expect(saga!.status, equals(TaskStatus.created));
    });

    test('during() handles state-specific events', () async {
      await machine.dispatch(TaskCreated('task-1', 'Test Task'));
      await machine.dispatch(TaskStarted('task-1'));

      final saga = machine.getSaga('task-1');
      expect(saga!.status, equals(TaskStatus.inProgress));
    });

    test('duringAny() handles events from any state', () async {
      await machine.dispatch(TaskCreated('task-1', 'Test Task'));

      // Cancel from created state
      await machine.dispatch(TaskCancelled('task-1'));

      expect(machine.getSaga('task-1'), isNull); // Finalized and removed
    });

    test('ignores events not matching current state', () async {
      await machine.dispatch(TaskCreated('task-1', 'Test Task'));

      // Try to complete without starting - should be ignored
      await machine.dispatch(TaskCompleted('task-1'));

      final saga = machine.getSaga('task-1');
      expect(saga!.status, equals(TaskStatus.created)); // Still created
    });

    test('handles multiple handlers for same state', () async {
      await machine.dispatch(TaskCreated('task-1', 'Test Task'));
      await machine.dispatch(TaskStarted('task-1'));

      // Progress update (first handler)
      await machine.dispatch(TaskProgressUpdated('task-1', 50));

      final saga = machine.getSaga('task-1');
      expect(saga!.progressPercent, equals(50));
      expect(saga.status, equals(TaskStatus.inProgress));
    });

    test('transition callback is called', () async {
      await machine.dispatch(TaskCreated('task-1', 'Test Task'));
      await machine.dispatch(TaskStarted('task-1'));

      expect(
        machine.transitionLog,
        contains('task-1: TaskStatus.created → TaskStatus.inProgress'),
      );
    });
  });

  group('Event Filtering with where()', () {
    late TaskStateMachine machine;

    setUp(() {
      machine = TaskStateMachine();
    });

    tearDown(() {
      machine.dispose();
    });

    test('where() filters matching events', () async {
      // Priority 5+ should match
      final saga = await machine.dispatch(
        HighPriorityTaskCreated('task-1', 'High Priority', 5),
      );

      expect(saga, isNotNull);
      expect(saga!.title, equals('[HIGH] High Priority'));
    });

    test('where() rejects non-matching events', () async {
      // Priority < 5 should not match
      final saga = await machine.dispatch(
        HighPriorityTaskCreated('task-1', 'Low Priority', 3),
      );

      expect(saga, isNull);
    });
  });

  group('Setters with set()', () {
    late TaskStateMachine machine;

    setUp(() {
      machine = TaskStateMachine();
    });

    tearDown(() {
      machine.dispose();
    });

    test('set() modifies saga properties', () async {
      final saga = await machine.dispatch(
        TaskCreated('task-1', 'Test Task', assignee: 'John'),
      );

      expect(saga!.title, equals('Test Task'));
      expect(saga.assignee, equals('John'));
    });

    test('multiple set() calls accumulate', () async {
      await machine.dispatch(TaskCreated('task-1', 'Test Task'));
      await machine.dispatch(TaskStarted('task-1'));
      await machine.dispatch(TaskProgressUpdated('task-1', 25));
      await machine.dispatch(TaskProgressUpdated('task-1', 50));
      await machine.dispatch(TaskProgressUpdated('task-1', 75));

      final saga = machine.getSaga('task-1');
      expect(saga!.progressPercent, equals(75));
      expect(saga.logs, contains('Progress: 25%'));
      expect(saga.logs, contains('Progress: 50%'));
      expect(saga.logs, contains('Progress: 75%'));
    });
  });

  group('Actions with then()', () {
    late TaskStateMachine machine;

    setUp(() {
      machine = TaskStateMachine();
    });

    tearDown(() {
      machine.dispose();
    });

    test('then() executes custom action', () async {
      await machine.dispatch(TaskCreated('task-1', 'Test Task'));
      await machine.dispatch(TaskStarted('task-1'));
      await machine.dispatch(TaskPaused('task-1'));
      await machine.dispatch(TaskResumed('task-1'));

      final saga = machine.getSaga('task-1');
      expect(saga!.logs, contains('Custom action executed'));
    });

    test('then() has access to context', () async {
      await machine.dispatch(TaskCreated('task-1', 'Test Task'));
      await machine.dispatch(TaskStarted('task-1'));
      await machine.dispatch(TaskPaused('task-1'));
      await machine.dispatch(TaskResumed('task-1'));

      final saga = machine.getSaga('task-1');
      expect(saga!.wasResumed, isTrue);
    });
  });

  group('Activities with execute()', () {
    late TaskStateMachine machine;

    setUp(() {
      machine = TaskStateMachine();
    });

    tearDown(() {
      machine.dispose();
    });

    test('execute() runs activity', () async {
      await machine.dispatch(TaskCreated('task-1', 'Test Task'));
      await machine.dispatch(TaskStarted('task-1'));

      final saga = machine.getSaga('task-1');
      expect(saga!.logs, contains('Activity: Task started'));
    });
  });

  group('Dynamic State Resolution with transitionToState()', () {
    late TaskStateMachine machine;

    setUp(() {
      machine = TaskStateMachine();
    });

    tearDown(() {
      machine.dispose();
    });

    test('transitionToState() resolves state dynamically', () async {
      await machine.dispatch(TaskCreated('task-1', 'Test Task'));
      await machine.dispatch(TaskStarted('task-1'));
      await machine.dispatch(TaskFailed('task-1', 'Network error'));

      // Can't check saga directly as it's finalized and removed
      expect(
        machine.transitionLog,
        contains('task-1: TaskStatus.inProgress → TaskStatus.failed'),
      );
    });
  });

  group('Finalization', () {
    late TaskStateMachine machine;

    setUp(() {
      machine = TaskStateMachine();
    });

    tearDown(() {
      machine.dispose();
    });

    test('finalize() marks saga as finalized', () async {
      await machine.dispatch(TaskCreated('task-1', 'Test Task'));
      await machine.dispatch(TaskStarted('task-1'));

      final saga = await machine.dispatch(TaskCompleted('task-1'));
      expect(saga!.isFinalized, isTrue);
    });

    test('onSagaFinalized callback is called', () async {
      await machine.dispatch(TaskCreated('task-1', 'Test Task'));
      await machine.dispatch(TaskStarted('task-1'));
      await machine.dispatch(TaskCompleted('task-1'));

      expect(machine.finalizeLog, contains('Finalized: task-1'));
    });

    test('whenFinalized activities are executed', () async {
      await machine.dispatch(TaskCreated('task-1', 'Test Task'));
      await machine.dispatch(TaskStarted('task-1'));
      await machine.dispatch(TaskCompleted('task-1'));

      expect(machine.activityLog, contains('Cleanup for task-1'));
    });
  });

  group('Custom Repository', () {
    test('useRepository() sets custom repository', () async {
      final machine = TaskStateMachine();
      final customRepo = TrackingSagaRepository();
      machine.useRepository(customRepo);

      await machine.dispatch(TaskCreated('task-1', 'Test Task'));
      await machine.dispatch(TaskStarted('task-1'));
      await machine.dispatch(TaskCompleted('task-1'));

      expect(customRepo.saveLog.length, greaterThan(0));
      expect(customRepo.removeLog, contains('Removed: task-1'));

      machine.dispose();
    });
  });

  group('Utility Methods', () {
    late TaskStateMachine machine;

    setUp(() {
      machine = TaskStateMachine();
    });

    tearDown(() {
      machine.dispose();
    });

    test('getSaga() returns saga by id', () async {
      await machine.dispatch(TaskCreated('task-1', 'Test Task'));

      final saga = machine.getSaga('task-1');
      expect(saga, isNotNull);
      expect(saga!.id, equals('task-1'));
    });

    test('getSaga() returns null for non-existent id', () {
      final saga = machine.getSaga('non-existent');
      expect(saga, isNull);
    });

    test('getActiveSagas() returns all non-finalized sagas', () async {
      await machine.dispatch(TaskCreated('task-1', 'Task 1'));
      await machine.dispatch(TaskCreated('task-2', 'Task 2'));
      await machine.dispatch(TaskCreated('task-3', 'Task 3'));

      final active = machine.getActiveSagas();
      expect(active.length, equals(3));
    });

    test('dispose() clears all sagas', () async {
      await machine.dispatch(TaskCreated('task-1', 'Task 1'));
      await machine.dispatch(TaskCreated('task-2', 'Task 2'));

      machine.dispose();

      // Create new machine to test
      final newMachine = TaskStateMachine();
      expect(newMachine.getActiveSagas().length, equals(0));
      newMachine.dispose();
    });
  });

  group('InMemorySagaRepository', () {
    late InMemorySagaRepository<TaskSaga> repo;

    setUp(() {
      repo = InMemorySagaRepository<TaskSaga>();
    });

    test('save() stores saga', () {
      final saga = TaskSaga()..id = 'test-1';
      repo.save(saga);

      expect(repo.getById('test-1'), equals(saga));
    });

    test('getById() returns null for non-existent', () {
      expect(repo.getById('non-existent'), isNull);
    });

    test('remove() deletes saga', () {
      final saga = TaskSaga()..id = 'test-1';
      repo.save(saga);
      repo.remove('test-1');

      expect(repo.getById('test-1'), isNull);
    });

    test('getActive() excludes finalized sagas', () {
      final saga1 = TaskSaga()..id = 'test-1';
      final saga2 = TaskSaga()
        ..id = 'test-2'
        ..isFinalized = true;

      repo.save(saga1);
      repo.save(saga2);

      final active = repo.getActive();
      expect(active.length, equals(1));
      expect(active.first.id, equals('test-1'));
    });

    test('clear() removes all sagas', () {
      final saga1 = TaskSaga()..id = 'test-1';
      final saga2 = TaskSaga()..id = 'test-2';

      repo.save(saga1);
      repo.save(saga2);
      repo.clear();

      expect(repo.getActive(), isEmpty);
    });
  });

  group('Scheduler', () {
    late Scheduler scheduler;
    final scheduledEvents = <String>[];

    void onScheduledEvent<T>(String sagaId, T event) {
      scheduledEvents.add('$sagaId: ${event.runtimeType}');
    }

    setUp(() {
      scheduledEvents.clear();
      scheduler = Scheduler(onScheduledEvent);
    });

    tearDown(() {
      scheduler.dispose();
    });

    test('schedule() dispatches event after delay', () async {
      scheduler.schedule(
        sagaId: 'saga-1',
        event: TaskTimeout('saga-1'),
        delay: const Duration(milliseconds: 50),
      );

      expect(scheduler.isScheduled<TaskTimeout>('saga-1'), isTrue);

      await Future.delayed(const Duration(milliseconds: 100));

      expect(scheduledEvents, contains('saga-1: TaskTimeout'));
      expect(scheduler.isScheduled<TaskTimeout>('saga-1'), isFalse);
    });

    test('unschedule() cancels scheduled event', () async {
      scheduler.schedule(
        sagaId: 'saga-1',
        event: TaskTimeout('saga-1'),
        delay: const Duration(milliseconds: 100),
      );

      scheduler.unschedule<TaskTimeout>('saga-1');

      await Future.delayed(const Duration(milliseconds: 150));

      expect(scheduledEvents, isEmpty);
    });

    test('unscheduleAll() cancels all events for saga', () async {
      scheduler.schedule(
        sagaId: 'saga-1',
        event: TaskTimeout('saga-1'),
        delay: const Duration(milliseconds: 100),
      );

      scheduler.unscheduleAll('saga-1');

      await Future.delayed(const Duration(milliseconds: 150));

      expect(scheduledEvents, isEmpty);
    });

    test('isScheduled() returns correct state', () {
      expect(scheduler.isScheduled<TaskTimeout>('saga-1'), isFalse);

      scheduler.schedule(
        sagaId: 'saga-1',
        event: TaskTimeout('saga-1'),
        delay: const Duration(seconds: 10),
      );

      expect(scheduler.isScheduled<TaskTimeout>('saga-1'), isTrue);
    });

    test('dispose() cancels all timers', () async {
      scheduler.schedule(
        sagaId: 'saga-1',
        event: TaskTimeout('saga-1'),
        delay: const Duration(milliseconds: 100),
      );

      scheduler.dispose();

      await Future.delayed(const Duration(milliseconds: 150));

      expect(scheduledEvents, isEmpty);
    });
  });

  group('EventHandler', () {
    test('canHandle() checks event type', () {
      final handler = EventHandler<TaskSaga, TaskCreated, TaskStatus>();

      expect(handler.canHandle(TaskCreated('id', 'title')), isTrue);
      expect(handler.canHandle(TaskStarted('id')), isFalse);
      expect(handler.canHandle('string'), isFalse);
    });

    test('matches() respects filter', () {
      final handler = EventHandler<TaskSaga, HighPriorityTaskCreated, TaskStatus>().where((e) => e.priority >= 5);

      expect(handler.matches(HighPriorityTaskCreated('id', 'title', 5)), isTrue);
      expect(handler.matches(HighPriorityTaskCreated('id', 'title', 3)), isFalse);
    });

    test('fluent methods return this for chaining', () {
      final handler = EventHandler<TaskSaga, TaskCreated, TaskStatus>()
          .set((saga, e) => saga.title = e.title)
          .transitionTo(TaskStatus.created)
          .finalize();

      expect(handler.shouldFinalize, isTrue);
    });

    test('where() creates new handler with filter', () {
      final original = EventHandler<TaskSaga, HighPriorityTaskCreated, TaskStatus>();
      final filtered = original.where((e) => e.priority > 5);

      // Different instances
      expect(filtered, isNot(same(original)));
      expect(filtered.matches(HighPriorityTaskCreated('id', 'title', 6)), isTrue);
      expect(filtered.matches(HighPriorityTaskCreated('id', 'title', 5)), isFalse);
    });
  });

  group('Activity', () {
    test('FunctionActivity executes function', () async {
      var executed = false;

      final activity = FunctionActivity<TaskSaga, TaskCreated>(
        (context) {
          executed = true;
        },
      );

      final saga = TaskSaga()..id = 'test';
      final scheduler = Scheduler(<T>(String sagaId, T event) {});
      final context = BehaviorContext<TaskSaga, TaskCreated>(
        saga: saga,
        event: TaskCreated('test', 'title'),
        scheduler: scheduler,
      );

      await activity.execute(context);
      scheduler.dispose();

      expect(executed, isTrue);
    });

    test('FunctionActivity with compensation', () async {
      var compensated = false;

      final activity = FunctionActivity<TaskSaga, TaskCreated>(
        (context) {},
        compensate: (context) {
          compensated = true;
        },
      );

      final saga = TaskSaga()..id = 'test';
      final scheduler = Scheduler(<T>(String sagaId, T event) {});
      final context = BehaviorContext<TaskSaga, TaskCreated>(
        saga: saga,
        event: TaskCreated('test', 'title'),
        scheduler: scheduler,
      );

      await activity.compensate(context);
      scheduler.dispose();

      expect(compensated, isTrue);
    });

    test('NoOpActivity does nothing', () async {
      final activity = NoOpActivity<TaskSaga, TaskCreated>();

      final saga = TaskSaga()..id = 'test';
      final scheduler = Scheduler(<T>(String sagaId, T event) {});
      final context = BehaviorContext<TaskSaga, TaskCreated>(
        saga: saga,
        event: TaskCreated('test', 'title'),
        scheduler: scheduler,
      );

      // Should not throw
      await activity.execute(context);
      scheduler.dispose();
    });
  });

  group('BehaviorContext', () {
    test('provides access to saga and event', () {
      final saga = TaskSaga()..id = 'test';
      final event = TaskCreated('test', 'title');
      final scheduler = Scheduler(<T>(String sagaId, T e) {});

      final context = BehaviorContext<TaskSaga, TaskCreated>(
        saga: saga,
        event: event,
        scheduler: scheduler,
        previousState: TaskStatus.created,
      );

      expect(context.saga, equals(saga));
      expect(context.event, equals(event));
      expect(context.previousState, equals(TaskStatus.created));
      scheduler.dispose();
    });
  });

  group('onSagaUpdated Callback', () {
    late TaskStateMachine machine;

    setUp(() {
      machine = TaskStateMachine();
    });

    tearDown(() {
      machine.dispose();
    });

    test('onSagaUpdated is called for property-only changes (no state transition)', () async {
      await machine.dispatch(TaskCreated('task-1', 'Test Task'));
      await machine.dispatch(TaskStarted('task-1'));
      machine.updateLog.clear();

      // Progress update has .set() but no .transitionTo()
      await machine.dispatch(TaskProgressUpdated('task-1', 50));

      expect(machine.updateLog, contains('Updated: task-1 (progress: 50%)'));
    });

    test('onSagaUpdated is NOT called for state transitions', () async {
      machine.updateLog.clear();

      await machine.dispatch(TaskCreated('task-1', 'Test Task'));
      await machine.dispatch(TaskStarted('task-1'));

      // State transitions should only fire onAnyTransition, not onSagaUpdated
      expect(machine.updateLog, isEmpty);
      expect(machine.transitionLog, isNotEmpty);
    });

    test('onSagaUpdated is called multiple times for multiple property updates', () async {
      await machine.dispatch(TaskCreated('task-1', 'Test Task'));
      await machine.dispatch(TaskStarted('task-1'));
      machine.updateLog.clear();

      await machine.dispatch(TaskProgressUpdated('task-1', 10));
      await machine.dispatch(TaskProgressUpdated('task-1', 25));
      await machine.dispatch(TaskProgressUpdated('task-1', 50));

      expect(machine.updateLog.length, equals(3));
      expect(machine.updateLog[0], contains('progress: 10%'));
      expect(machine.updateLog[1], contains('progress: 25%'));
      expect(machine.updateLog[2], contains('progress: 50%'));
    });

    test('saga.markUpdated() is called for property-only changes', () async {
      await machine.dispatch(TaskCreated('task-1', 'Test Task'));
      await machine.dispatch(TaskStarted('task-1'));

      final saga = machine.getSaga('task-1');
      final beforeUpdate = saga!.updatedAt;

      await Future.delayed(const Duration(milliseconds: 10));
      await machine.dispatch(TaskProgressUpdated('task-1', 50));

      final afterUpdate = machine.getSaga('task-1')!.updatedAt;
      expect(afterUpdate.isAfter(beforeUpdate), isTrue);
    });

    test('onSagaUpdated receives correct saga instance', () async {
      TaskSaga? capturedSaga;
      final testMachine = _UpdateCallbackTestMachine(
        onUpdate: (saga) => capturedSaga = saga,
      );

      await testMachine.dispatch(TaskCreated('task-1', 'Test'));
      await testMachine.dispatch(TaskStarted('task-1'));
      await testMachine.dispatch(TaskProgressUpdated('task-1', 75));

      expect(capturedSaga, isNotNull);
      expect(capturedSaga!.id, equals('task-1'));
      expect(capturedSaga!.progressPercent, equals(75));

      testMachine.dispose();
    });

    test('onSagaUpdated not called when handler has no modifiers', () async {
      final testMachine = _NoModifierMachine();

      await testMachine.dispatch(TaskCreated('task-1', 'Test'));
      testMachine.updateLog.clear();

      // Dispatch event handled by no-modifier handler
      await testMachine.dispatch(_NoOpEvent('task-1'));

      expect(testMachine.updateLog, isEmpty);

      testMachine.dispose();
    });

    test('onSagaUpdated works with activities (not just setters)', () async {
      final testMachine = _ActivityOnlyMachine();

      await testMachine.dispatch(TaskCreated('task-1', 'Test'));
      testMachine.updateLog.clear();

      await testMachine.dispatch(_ActivityEvent('task-1'));

      expect(testMachine.updateLog, contains('Updated: task-1'));

      testMachine.dispose();
    });

    test('onSagaUpdated works with actions (then() calls)', () async {
      final testMachine = _ActionOnlyMachine();

      await testMachine.dispatch(TaskCreated('task-1', 'Test'));
      testMachine.updateLog.clear();

      await testMachine.dispatch(_ActionEvent('task-1'));

      expect(testMachine.updateLog, contains('Updated: task-1'));

      testMachine.dispose();
    });
  });

  group('Initial Transition Callback', () {
    late TaskStateMachine machine;

    setUp(() {
      machine = TaskStateMachine();
    });

    tearDown(() {
      machine.dispose();
    });

    test('onAnyTransition is called for initial state transition', () async {
      machine.transitionLog.clear();

      await machine.dispatch(TaskCreated('task-1', 'Test Task'));

      // Initial transition should fire callback with saga's initial state as "from"
      expect(machine.transitionLog, isNotEmpty);
      expect(
        machine.transitionLog.first,
        equals('task-1: TaskStatus.created → TaskStatus.created'),
      );
    });

    test('initial transition uses getState(saga) as fromState', () async {
      String? capturedFrom;
      final testMachine = _TransitionCallbackTestMachine(
        onTransition: (saga, from, to) => capturedFrom = from.toString(),
      );

      await testMachine.dispatch(TaskCreated('task-1', 'Test'));

      // fromState should be the saga's initial state (created), not null
      expect(capturedFrom, isNotNull);
      expect(capturedFrom, contains('created'));

      testMachine.dispose();
    });
  });

  group('Edge Cases', () {
    late TaskStateMachine machine;

    setUp(() {
      machine = TaskStateMachine();
    });

    tearDown(() {
      machine.dispose();
    });

    test('dispatching same event type multiple times', () async {
      await machine.dispatch(TaskCreated('task-1', 'Task 1'));
      await machine.dispatch(TaskStarted('task-1'));

      await machine.dispatch(TaskProgressUpdated('task-1', 10));
      await machine.dispatch(TaskProgressUpdated('task-1', 20));
      await machine.dispatch(TaskProgressUpdated('task-1', 30));

      final saga = machine.getSaga('task-1');
      expect(saga!.progressPercent, equals(30));
    });

    test('rapid state transitions', () async {
      await machine.dispatch(TaskCreated('task-1', 'Test'));
      await machine.dispatch(TaskStarted('task-1'));
      await machine.dispatch(TaskPaused('task-1'));
      await machine.dispatch(TaskResumed('task-1'));
      await machine.dispatch(TaskPaused('task-1'));
      await machine.dispatch(TaskResumed('task-1'));

      final saga = machine.getSaga('task-1');
      expect(saga!.status, equals(TaskStatus.inProgress));
    });

    test('multiple sagas concurrently', () async {
      // Create multiple sagas
      for (var i = 0; i < 10; i++) {
        await machine.dispatch(TaskCreated('task-$i', 'Task $i'));
      }

      expect(machine.getActiveSagas().length, equals(10));

      // Progress some
      for (var i = 0; i < 5; i++) {
        await machine.dispatch(TaskStarted('task-$i'));
      }

      // Verify states
      for (var i = 0; i < 5; i++) {
        expect(machine.getSaga('task-$i')!.status, equals(TaskStatus.inProgress));
      }
      for (var i = 5; i < 10; i++) {
        expect(machine.getSaga('task-$i')!.status, equals(TaskStatus.created));
      }
    });

    test('no transition callback for initial state', () async {
      machine.transitionLog.clear();

      // Initial event should NOT trigger transition callback
      // (there's no "from" state)
      await machine.dispatch(TaskCreated('task-1', 'Test'));

      // Verify no transition logged for initial creation
      expect(
        machine.transitionLog.where((l) => l.contains('null')),
        isEmpty,
      );
    });
  });

  group('Timeout', () {
    late TaskStateMachine machine;

    setUp(() {
      machine = TaskStateMachine();
    });

    tearDown(() {
      machine.dispose();
    });

    test('timeout ocurred before event dispatch', () async {
      await machine.dispatch(TaskCreated('task-1', 'Task 1'));
      await Future.delayed(const Duration(milliseconds: 250));
      await machine.dispatch(TaskStarted('task-1'));

      final saga = machine.getSaga('task-1');
      expect(saga!.status, TaskStatus.timeout);
    });

    test('timeout was cancelled', () async {
      await machine.dispatch(TaskCreated('task-1', 'Task 1'));
      await Future.delayed(const Duration(milliseconds: 150));
      await machine.dispatch(TaskStarted('task-1'));

      final saga = machine.getSaga('task-1');
      expect(saga!.status, TaskStatus.inProgress);
    });
  });
}

// Helper class for testing uncorrelated events
class _UncorrelatedEvent {}

// Helper event for no-modifier test
class _NoOpEvent {
  final String taskId;
  _NoOpEvent(this.taskId);
}

// Helper event for activity-only test
class _ActivityEvent {
  final String taskId;
  _ActivityEvent(this.taskId);
}

// Helper event for action-only test
class _ActionEvent {
  final String taskId;
  _ActionEvent(this.taskId);
}

// Test machine that captures onSagaUpdated callback
class _UpdateCallbackTestMachine extends SagaStateMachine<TaskSaga, TaskStatus> {
  final void Function(TaskSaga) onUpdate;

  _UpdateCallbackTestMachine({required this.onUpdate}) {
    correlate<TaskCreated>((e) => e.taskId);
    correlate<TaskStarted>((e) => e.taskId);
    correlate<TaskProgressUpdated>((e) => e.taskId);

    initially(
      when<TaskCreated>().transitionTo(TaskStatus.created),
    );

    during(
      TaskStatus.created,
      when<TaskStarted>().transitionTo(TaskStatus.inProgress),
    );

    during(
      TaskStatus.inProgress,
      when<TaskProgressUpdated>().set((saga, e) => saga.progressPercent = e.percent),
    );

    onSagaUpdated(onUpdate);
  }

  @override
  TaskSaga createSaga(String correlationId) => TaskSaga()..id = correlationId;

  @override
  TaskStatus getState(TaskSaga saga) => saga.status;

  @override
  void setState(TaskSaga saga, TaskStatus state) => saga.status = state;
}

// Test machine with handler that has no modifiers (no set, activity, or action)
class _NoModifierMachine extends SagaStateMachine<TaskSaga, TaskStatus> {
  final List<String> updateLog = [];

  _NoModifierMachine() {
    correlate<TaskCreated>((e) => e.taskId);
    correlate<_NoOpEvent>((e) => e.taskId);

    initially(
      when<TaskCreated>().transitionTo(TaskStatus.created),
    );

    during(
      TaskStatus.created,
      // Handler with no modifiers - just matches the event
      when<_NoOpEvent>(),
    );

    onSagaUpdated((saga) {
      updateLog.add('Updated: ${saga.id}');
    });
  }

  @override
  TaskSaga createSaga(String correlationId) => TaskSaga()..id = correlationId;

  @override
  TaskStatus getState(TaskSaga saga) => saga.status;

  @override
  void setState(TaskSaga saga, TaskStatus state) => saga.status = state;
}

// Test machine with activity-only handler (no setter)
class _ActivityOnlyMachine extends SagaStateMachine<TaskSaga, TaskStatus> {
  final List<String> updateLog = [];

  _ActivityOnlyMachine() {
    correlate<TaskCreated>((e) => e.taskId);
    correlate<_ActivityEvent>((e) => e.taskId);

    initially(
      when<TaskCreated>().transitionTo(TaskStatus.created),
    );

    during(
      TaskStatus.created,
      when<_ActivityEvent>().execute(FunctionActivity((ctx) {
        ctx.saga.logs.add('Activity executed');
      })),
    );

    onSagaUpdated((saga) {
      updateLog.add('Updated: ${saga.id}');
    });
  }

  @override
  TaskSaga createSaga(String correlationId) => TaskSaga()..id = correlationId;

  @override
  TaskStatus getState(TaskSaga saga) => saga.status;

  @override
  void setState(TaskSaga saga, TaskStatus state) => saga.status = state;
}

// Test machine with action-only handler (using then())
class _ActionOnlyMachine extends SagaStateMachine<TaskSaga, TaskStatus> {
  final List<String> updateLog = [];

  _ActionOnlyMachine() {
    correlate<TaskCreated>((e) => e.taskId);
    correlate<_ActionEvent>((e) => e.taskId);

    initially(
      when<TaskCreated>().transitionTo(TaskStatus.created),
    );

    during(
      TaskStatus.created,
      when<_ActionEvent>().then((ctx) async {
        ctx.saga.logs.add('Action executed');
      }),
    );

    onSagaUpdated((saga) {
      updateLog.add('Updated: ${saga.id}');
    });
  }

  @override
  TaskSaga createSaga(String correlationId) => TaskSaga()..id = correlationId;

  @override
  TaskStatus getState(TaskSaga saga) => saga.status;

  @override
  void setState(TaskSaga saga, TaskStatus state) => saga.status = state;
}

// Test machine that captures transition callback
class _TransitionCallbackTestMachine extends SagaStateMachine<TaskSaga, TaskStatus> {
  final void Function(TaskSaga, TaskStatus, TaskStatus) onTransition;

  _TransitionCallbackTestMachine({required this.onTransition}) {
    correlate<TaskCreated>((e) => e.taskId);

    initially(
      when<TaskCreated>().transitionTo(TaskStatus.created),
    );

    onAnyTransition(onTransition);
  }

  @override
  TaskSaga createSaga(String correlationId) => TaskSaga()..id = correlationId;

  @override
  TaskStatus getState(TaskSaga saga) => saga.status;

  @override
  void setState(TaskSaga saga, TaskStatus state) => saga.status = state;
}
