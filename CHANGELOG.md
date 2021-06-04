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
