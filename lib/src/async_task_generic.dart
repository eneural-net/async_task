import 'async_task_base.dart';

int getAsyncExecutorMaximumParallelism() => 1;

AsyncExecutorThread? createMultiThreadAsyncExecutorThread(
    AsyncTaskLoggerCaller logger, bool sequential, int parallelism,
    [AsyncTaskRegister? taskRegister]) {
  return null;
}
