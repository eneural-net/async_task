## 1.1.5

- async_extension: ^1.2.24

## 1.1.4

- `lib/src/async_task_isolate.dart`:
  - `_Isolate`: an error while resolving a task's `SharedData` in the `Isolate`
    (`loadSharedData` throwing or returning `null`, or a task type not registered)
    now completes the task with an `AsyncExecutorError`.
    Previously the error was unhandled in the `Isolate` port handler, killing the `Isolate`:
    the task never completed and `AsyncExecutor.close` never returned.
  - `_Isolate`: a failed request of a missing `SharedData` (to the main side) no longer
    leaves a stale pending request: the next task requests it again.
  - `_Isolate`: the task channel port is sent to the main side only after the task channel is initialized.
    When the task execution was delayed by a `SharedData` request to the main side,
    channel messages sent by the main side in the meantime were lost (the task waited forever).
  - `_Isolate`: releases the task channel port of a task that failed resolving its `SharedData`.

- Tests:
  - New `async_task_stress_test.dart` (VM and Web): hundreds of tasks with `parallelism` 0/1/4,
    sequential and not, mixed sync/async/throwing tasks, results order and identity,
    `close` while tasks are queued or running, `execute` while closing,
    `SharedData` reuse and dispose across many tasks and bidirectional channel messages under load.
  - New `async_task_isolate_integration_test.dart` (VM): `SharedData` load failures and
    the request of a missing `SharedData` from the main side, dispose between queued tasks,
    concurrent tasks with new `SharedData` and `Isolate` termination on close (via `vm_service`).
  - `async_task_counter_test.dart`: replaced the wall-clock assertions (execution and total time thresholds)
    with assertions on the ordering of the execution intervals (concurrent vs. sequential execution).

## 1.1.3

- `lib/src/async_task_base.dart`:
  - `_AsyncExecutorSingleThread.execute` (used with `parallelism: 0` and on the Web): marks the task as submitted immediately,
    as the `Isolate` executor does, so a repeated `AsyncExecutor.execute` of the same task no longer executes it twice.
  - `AsyncTask`: a throwing `OnFinishAsyncTask` trigger now logs its own error (was logging the task error).

- `lib/src/async_task_shared_data.dart`:
  - `SerializableData.copyGeneric`: fixed `Int16List` and `Float64List` copies (were creating an `Int8List`/`Float32List` and failing).
  - `SerializableData.copyGeneric`, `serializeGeneric`, `deserializeGeneric`: `MapEntry` now keeps its `value` (the `key` was used for both).
  - `SerializableData.deserializeGeneric`: `dynamic`/`Object` types return the value as is,
    allowing the deserialization of untyped `List`, `Set`, `Iterable` and `MapEntry` elements.

- `lib/src/async_task_extension.dart`:
  - `ListDoubleExtension.toFloat64x2List`: fixed index computation (was failing for lists with more than 2 values).

- `lib/src/async_task_shared_pointer.dart`:
  - `SharedPointerAddress.serialize`: fixed the order of the serialized fields, matching `deserialize`.

- `lib/src/async_task_channel.dart`:
  - `AsyncTaskChannel` constructor: uses a private initializing formal (the named parameter is still `messageHandler`).

- Removed library names (`library;`) and fixed dartdoc references.
- `dart format` with the Dart 3.13 style.

- Tests:
  - New `async_task_executor_test.dart` (VM and Web), `async_task_isolate_test.dart` (VM) and `async_task_shared_data_test.dart`.
  - Close all executors at the end of the tests.

- `pubspec.yaml`:
  - Updated SDK constraint to `^3.13.0`.
  - Updated dependencies:
    - `async_extension` to ^1.2.23
    - `ffi` to ^2.2.0
  - Updated dev_dependencies:
    - `lints` to ^6.1.0
    - `test` to ^1.33.0
    - `dependency_validator` to ^5.1.0
    - `coverage` to ^1.16.0
    - `vm_service` to ^15.4.0

- `.github/workflows/dart.yml`:
  - `actions/checkout@v7`.
  - `codecov/codecov-action@v7`.

## 1.1.2

- `lib/src/async_task_isolate.dart`:
  - `_Isolate` class `_handleMessage` method: removed default case that threw `StateError` on unhandled messages (not needed in Dart 3.6+).

- `pubspec.yaml`:
  - Updated SDK constraint to `>=3.6.0 <4.0.0`.
  - Updated dependencies:
    - `collection` to ^1.19.1
    - `async_extension` to ^1.2.20
    - `ffi` to ^2.1.5
  - Updated dev_dependencies:
    - `test` to ^1.29.0
    - `coverage` to ^1.15.0

## 1.1.2.beta

- async_extension: ^1.2.12
- ffi: ^2.1.3
- test: ^1.25.8
- coverage: ^1.9.2

## 1.1.1

- Optimize `AsyncTaskLoggerCaller`:
  - Avoid `String` build when logging is disabled.
- Optimize `async_task_isolate.dart`:
  - Improve message objects, to allow immutable detection.
- `AsyncTask`:
  - Improve error handling and error response from `Isolate`. 

- sdk: '>=3.2.0 <4.0.0'
- async_extension: ^1.2.5
- lints: ^3.0.0

- test: ^1.24.9
- dependency_validator: ^3.2.3
- coverage: ^1.7.1
- vm_service: ^13.0.0

## 1.1.0

- sdk: '>=3.0.0 <4.0.0'

- collection: ^1.18.0
- async_extension: ^1.2.4
- ffi: ^2.1.0
- lints: ^2.1.1
- test: ^1.24.6
- coverage: ^1.6.3
- vm_service: ^11.9.0

## 1.0.20

- `README.md`:
  - Fix GitHub CI badge.
  - Added `parallelism` explanation.
- test: ^1.22.1

## 1.0.19

- Update GitHub CI.
  - Added tests in chrome.
- sdk: '>=2.17.0 <3.0.0'
- collection: ^1.17.0
- async_extension: ^1.0.12
- ffi: ^2.0.1
- lints: ^2.0.1
- test: ^1.22.0
- dependency_validator: ^3.2.2
- coverage: ^1.6.1
- vm_service: ^9.4.0

## 1.0.18

- `AsyncExecutor`:
  - Added field `name` (for debug purposes).
  - Now detects Isolate starting timeout. 

## 1.0.17

- New `AsyncExecutorThreadInfo` and `AsyncThreadInfo`.
- Improved `AsyncTaskChannel` close behavior.
- Reduced allocation of closures and `Future`s to improve GC performance.
- `AsyncTask`:
  - Added trigger list `addOnFinishAsyncTask`, to avoid `Future`s.

## 1.0.16

- `SharedPointerBytes`:
  - Expose `bytes`and `byteData`.

## 1.0.15

- Added `SharedPointer`: shared memory area between `Isolate`s.
- ffi: ^1.1.2

## 1.0.14

- Fix issue when reusing `_ReceivePort`.
- Optimize imports.

## 1.0.13

- `AsyncTaskChannel`:
  - Added `id` to help debugging.
  - Added non-blocking `readMessage`.
  - Added `messageQueueLength` and `messageQueueIsEmpty`.
- `AsyncTaskChannelPort`:
  - Added `id` to help debugging.
  - Added  `readSync` and `messageQueueLength`.
- Fixed issue executing tasks in a not start `AsyncExecutor` that is sequencial.
- async_extension: ^1.0.9
- test: ^1.19.5
- dependency_validator: ^3.1.2

## 1.0.12

- Migrate from `pedantic` to `lints`.
- Using Dart coverage.
- async_extension: ^1.0.8
- lints: ^1.0.1
- coverage: ^1.0.3

## 1.0.11

- Improve tests.
- Only export `async_extension` on library `async_task_extension.dart`.

## 1.0.10

- Optimize `async` methods with `async_extension`.
- Improve tests.
- async_extension: ^1.0.4

## 1.0.9

- Ensure that all ports are closed after close executors.
- `README.md`:
  - Fix typo.
  - Improve channel usage description.
- Move `IterableFutureOrExtension` and `IterableFutureExtension` to
  package `async_extension`.
  - async_extension: ^1.0.3
  
## 1.0.8

- Fix isolate message of a task without `SharedData`.
- Improved tests scenarios.

## 1.0.7

- Small fix in README.
- Small fix in example.

## 1.0.6

- Added `AsyncTaskChannel` for messages communication with tasks during execution.
- Added `AsyncExecutorStatus`.
- `AsyncExecutor`: optimize to avoid creating of futures while executing/processing a task.
- Improved README. 

## 1.0.5

- Added `AsyncTaskPlatform` and `AsyncTaskPlatformType`.
- `AsyncTask`:
  - Optimize `taskType`
  - Optimize `execute` to use less `async` operations.
- Added `AsyncExecutorSharedDataInfo` to report `SharedData` information.
- `AsyncExecutor`:
  - New constructor parameter `parallelismPercentage`. 
  - Optimize `execute`, `executeAll` and `executeAllAndWaitResults` to dispatch less asyn operations.
  - Added `disposeSharedData` and `disposeSharedDataInfo`.
- Extensions:
  - Added `IterableFutureOrExtension` and `IterableFutureExtension`.
- `_AsyncExecutorMultiThread`:
  - Optimized to use less `async` operations.
  - Using `_RawReceivePortPool` to optimize ports.
  - Optimized to pre send `SharedData`.
- Added `_RawReceivePortPool`: pool of reusable `_ReceivePort`.
- Added `_ReceivePort`: an optimized `RawReceivePort`.

## 1.0.4

- `AsyncTask`:
  - Allow multiple `SharedData`: Optional method `sharedData`
    now returns a `Map<String,SharedData>`.

## 1.0.3

- `AsyncExecutor`:
  - Fix `close` operation while tasks are being executed.

## 1.0.2

- Added `SharedData`, to optimize data sharing between
  tasks and threads/isolates.
- `AsyncExecutor`: added `close` to stop and finalize an executor.
- Added collections extensions:
  `ListExtension`, `MapExtension`, `SetExtension`, `IterableExtension`.

## 1.0.1

- Fix `pubspec.yaml` description length.
- Improve `README.md` description. 

## 1.0.0

- Implemented `AsyncTask` with `status`, `result` and `executionTime`. 
- `AsyncExecutor` with implementations based on `dart:isolate` and `dart:async`. 
- Initial version.
