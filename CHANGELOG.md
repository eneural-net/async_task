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

- Added `SharedPointer`: shared a memory aread between `Isolate`s.
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
