# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.1] - 2026-03-14

### Fixed

- Restored state timeout execution so configured `timeout()` handlers now fire and transition sagas as expected
- Registered internal timeout events for correlation, allowing scheduled timeout events to resolve the correct saga instance

### Changed

- Extended `EventHandler` construction options so internal timeout handlers preserve target state, finalization, and activities configuration

## [1.1.0] - 2026-01-19

### Added

- `onSagaUpdated()` callback - Register listener for property-only changes (no state transition)
- `UpdateCallback<TSaga>` typedef for saga update notifications
- `EventHandler.hasModifiers` getter - Check if handler has setters, activities, or actions

### Fixed

- Initial state transitions now correctly fire `onAnyTransition` callback
  - Previously, the first transition (e.g., idle → waiting) was silent because `currentState` was null
  - Now uses `getState(saga)` as `fromState` when `currentState` is null

### Changed

- `_executeHandler` now notifies `onSagaUpdated` when handlers have setters, activities, or actions but no state transition
  - Enables UI updates for property changes like mute toggle, hold toggle, duration updates
  - Follows MassTransit pattern where `.Set()` / `.Then()` without `TransitionTo()` still persists changes

## [1.0.1] - 2026-01-17

### Changed

- Enhanced README with detailed MassTransit comparison table
- Added side-by-side code examples (C# vs Dart)
- Added "Use Cases" section with common scenarios
- Added "Advanced Features" section with custom Activity and Repository examples
- Improved attribution and links to MassTransit documentation

### Fixed

- Updated LICENSE copyright to saga_state_machine contributors

## [1.0.0] - 2026-01-17

### Added

- Initial release of saga_state_machine
- `SagaStateMachine` - MassTransit-style declarative state machine
- `Saga` - Base class for saga instances with lifecycle tracking
- `EventHandler` - Fluent builder API for event handling
  - `when<E>()` - Create handler for event type
  - `where()` - Filter events by condition
  - `set()` - Set saga properties from event
  - `then()` - Execute custom async actions
  - `execute()` - Run activities with optional compensation
  - `transitionTo()` - Static state transitions
  - `transitionToState()` - Dynamic state resolution
  - `finalize()` - Mark saga as complete
  - `schedule()` / `unschedule()` - Delayed event scheduling
- `Activity` - Side effect abstraction with compensation support
  - `FunctionActivity` - Inline activity from function
  - `NoOpActivity` - Placeholder activity
- `Scheduler` - Timer-based event scheduling
- `SagaRepository` - Pluggable persistence interface
  - `InMemorySagaRepository` - Default in-memory implementation
- `BehaviorContext` - Context passed to actions and activities
- Event correlation via `correlate<E>()`
- State handlers: `initially()`, `during()`, `duringAny()`
- Timeout support for states
- Transition and finalization callbacks
- Comprehensive test suite (52 tests)
- Two complete examples:
  - Order processing workflow
  - VoIP call state management
