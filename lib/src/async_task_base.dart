import 'dart:async';
import 'dart:math' as math;

import 'package:collection/collection.dart';

import 'async_task_generic.dart'
    if (dart.library.isolate) 'async_task_isolate.dart';

/// Status of an [AsyncTask].
enum AsyncTaskStatus {
  idle,
  executing,
  successful,
  error,
}

/// Base class for tasks implementation.
abstract class AsyncTask<P, R> {
  final Completer<R> _completer = Completer<R>();

  String get taskType => '$runtimeType';

  R? _result;

  /// The result, after the task execution is finished.
  ///
  /// Returns `null` when task is not finished.
  R? get result => _result;

  dynamic _error;

  /// The error, after the task execution is finished.
  ///
  /// Returns `null` when task is not finished.
  dynamic get error => _error;

  bool _finished = false;

  /// Returns `true` if this task execution has finished.
  bool get isFinished => _finished;

  /// Returns `true` if this task execution has NOT finished.
  bool get isNotFinished => !_finished;

  /// Returns `true` if this task execution has finished successfully.
  bool get isSuccessful => isFinished && _error == null;

  /// Returns `true` if this task execution has finished with an error.
  bool get hasError => isFinished && _error != null;

  /// Executes this tasks immediately.
  FutureOr<R> execute() async {
    _initTime = DateTime.now();

    submitTime ??= _initTime;

    try {
      var ret = await run();
      _finish(ret, endTime: DateTime.now());
      return ret;
    } catch (e, s) {
      _finishError(e, s, endTime: DateTime.now());
      rethrow;
    }
  }

  void _finish(R result, {DateTime? initTime, DateTime? endTime}) {
    _setExecutionTime(initTime, endTime);
    _result = result;
    _finished = true;
    _completer.complete(result);
  }

  void _finishError(Object error, StackTrace? stackTrace,
      {DateTime? initTime, DateTime? endTime}) {
    _setExecutionTime(initTime, endTime);
    _error = error;
    _finished = true;
    _completer.completeError(error, stackTrace);
  }

  void _setExecutionTime(DateTime? initTime, DateTime? endTime) {
    if (initTime != null) {
      _initTime = initTime;
    }

    if (endTime != null) {
      _endTime = endTime;
    }
  }

  /// The submit time.
  DateTime? submitTime;

  /// Returns `true` if this task has been submitted.
  bool get wasSubmitted => submitTime != null;

  /// Returns `true` if this task has NOT yet been submitted.
  bool get isIdle => submitTime == null;

  DateTime? _initTime;

  /// The execution initial time.
  DateTime? get initTime => _initTime;

  DateTime? _endTime;

  /// The execution end time.
  DateTime? get endTime => _endTime;

  Duration? get executionTime => _initTime != null && _endTime != null
      ? Duration(
          milliseconds: _endTime!.millisecondsSinceEpoch -
              _initTime!.millisecondsSinceEpoch)
      : null;

  /// The current status.
  AsyncTaskStatus get status {
    if (hasError) {
      return AsyncTaskStatus.error;
    } else if (isSuccessful) {
      return AsyncTaskStatus.successful;
    } else if (wasSubmitted) {
      return AsyncTaskStatus.executing;
    } else {
      return AsyncTaskStatus.idle;
    }
  }

  /// The parameters of this tasks.
  ///
  /// - NOTE: [P] should be a type that can be sent through an [Isolate] or
  ///   serialized as JSON.
  P parameters();

  /// Creates an instance of this task type with [parameters].
  AsyncTask<P, R> instantiate(P parameters);

  /// Computes/runs this task.
  FutureOr<R> run();

  /// Returns a [Future] to wait for the task result.
  Future<R> waitResult() => _completer.future;

  @override
  String toString() {
    var status = this.status;

    var parametersStr = _toTrimmedString(parameters());
    var prefix = '$taskType($parametersStr)';

    switch (status) {
      case AsyncTaskStatus.idle:
        return '$prefix[idle]';
      case AsyncTaskStatus.executing:
        return '$prefix[executing]{ submitTime: $submitTime }';
      case AsyncTaskStatus.successful:
        return '$prefix[successful]{ result: ${_toTrimmedString(result)} ; executionTime: ${executionTime!.inMilliseconds} ms }';
      case AsyncTaskStatus.error:
        return '$prefix[error]{ error: $error ; submitTime: $submitTime }';
    }
  }

  String _toTrimmedString(dynamic o, [int limit = 10]) {
    var s = '$o';
    if (s.length > limit) s = s.substring(0, limit) + '...';
    return s;
  }
}

AsyncExecutorThread createAsyncExecutorThread(
    AsyncTaskLoggerCaller logger, bool sequential, int parallelism,
    [AsyncTaskRegister? taskRegister]) {
  if (parallelism >= 1) {
    return (createMultiThreadAsyncExecutorThread(
            logger, sequential, parallelism, taskRegister) ??
        _AsyncExecutorSingleThread(logger, sequential));
  } else {
    return _AsyncExecutorSingleThread(logger, sequential);
  }
}

typedef AsyncTaskLogger = void Function(String type, dynamic message,
    [dynamic error, dynamic stackTrace]);

final AsyncTaskLogger DefaultAsyncTaskLogger =
    (String type, dynamic message, [dynamic error, dynamic stackTrace]) {
  if (error != null) {
    if (message != null) {
      print('[$type] $message');

      if (error != null) {
        print(error);
      }

      if (stackTrace != null) {
        print(stackTrace);
      }
    } else {
      print('[$type] $error');

      if (stackTrace != null) {
        print(stackTrace);
      }
    }
  } else {
    print('[$type] $message');
  }
};

class AsyncTaskLoggerCaller {
  final AsyncTaskLogger _logger;

  AsyncTaskLoggerCaller(AsyncTaskLogger? logger)
      : _logger = logger ?? DefaultAsyncTaskLogger;

  bool enabled = false;

  void log(String type, dynamic message, [dynamic error, dynamic stackTrace]) {
    if (enabled) {
      _logger(type, message, error, stackTrace);
    }
  }

  void logInfo(dynamic message) => log('INFO', message);

  void logWarn(dynamic message) => log('WARN', message);

  void logError(dynamic error, dynamic stackTrace) =>
      log('ERROR', null, error, stackTrace);

  bool enabledExecution = false;

  void logExecution(dynamic message, dynamic thread, dynamic task) {
    if (enabledExecution) {
      log('EXEC', '$message > $thread > $task');
    }
  }
}

typedef AsyncTaskRegister = FutureOr<List<AsyncTask>> Function();

/// Asynchronous Executor of [AsyncTask].
class AsyncExecutor {
  /// If `true` the tasks will be executed sequentially, waiting each
  /// task to finished before start the next, in the order of [execute] call.
  final bool sequential;

  /// Number of parallel executors (like Threads) to execute the tasks.
  final int parallelism;

  /// A top-level function that returns the tasks types that can be executed
  /// by this executor.
  ///
  /// It should return [AsyncTask] instances just as samples of the tasks types
  /// (this instances won't be executed).
  final AsyncTaskRegister? taskTypeRegister;

  final AsyncExecutorThread _executorThread;

  late final AsyncTaskLoggerCaller _logger;

  AsyncExecutor(
      {bool sequential = false,
      int parallelism = 1,
      this.taskTypeRegister,
      AsyncTaskLogger? logger})
      : sequential = sequential,
        parallelism = math.max(0, parallelism),
        _executorThread = createAsyncExecutorThread(
            AsyncTaskLoggerCaller(logger),
            sequential,
            math.max(0, parallelism),
            taskTypeRegister) {
    _logger = _executorThread.logger;
  }

  AsyncTaskLoggerCaller get logger => _logger;

  bool _started = false;
  Completer<bool>? _starting;

  /// Starts this executor. Any call to [execute] will call [start] if this
  /// is not started yet.
  Future<bool> start() async {
    if (_started) return true;

    if (_starting != null) {
      return _starting!.future;
    }

    var starting = _starting = Completer<bool>();

    _logger.logInfo('Starting $this');

    await _executorThread.start();

    _started = true;

    starting.complete(true);
    return starting.future;
  }

  /// Execute [task] using this executor.
  ///
  /// If task was already submitted the execution will be skipped.
  Future<R> execute<P, R>(AsyncTask<P, R> task) async {
    if (!_started) {
      await start();
    }

    if (task.wasSubmitted) {
      return task.waitResult();
    }

    return _executorThread.execute(task);
  }

  /// Executes all the [tasks] using this executor. Calling [execute]
  /// for each task.
  List<Future<R>> executeAll<P, R>(Iterable<AsyncTask<P, R>> tasks) =>
      tasks.map((t) => execute(t)).toList();

  /// Executes all the [tasks] using this executor and waits for the results.
  Future<List<R>> executeAllAndWaitResults<P, R>(
      Iterable<AsyncTask<P, R>> tasks) async {
    var executions = executeAll(tasks);
    var results = await Future.wait(executions);
    return results;
  }

  @override
  String toString() {
    return 'AsyncExecutor{ sequential: $sequential, parallelism: $parallelism, executorThread: $_executorThread }';
  }
}

/// Base class for executor thread implementation.
abstract class AsyncExecutorThread {
  final AsyncTaskLoggerCaller logger;
  final bool sequential;

  AsyncExecutorThread(this.logger, this.sequential);

  /// Starts this thread.
  Future<bool> start();

  /// Executes [task] in this thread.
  Future<R> execute<P, R>(AsyncTask<P, R> task);

  /// Perform a task finish operation.
  void finishTask<P, R>(
      AsyncTask<P, R> task, DateTime? initTime, DateTime? endTime, R result,
      [dynamic error, StackTrace? stackTrace]) {
    if (!task.isFinished) {
      if (error != null) {
        task._finishError(error, stackTrace,
            initTime: initTime, endTime: endTime);
      } else {
        task._finish(result, initTime: initTime, endTime: endTime);
      }
    }
  }
}

class _AsyncExecutorSingleThread extends AsyncExecutorThread {
  _AsyncExecutorSingleThread(AsyncTaskLoggerCaller logger, bool sequential)
      : super(logger, sequential);

  @override
  Future<bool> start() async => true;

  final QueueList<AsyncTask> _queue = QueueList<AsyncTask>(32);
  AsyncTask? _executing;

  @override
  Future<R> execute<P, R>(AsyncTask<P, R> task) async {
    if (sequential) {
      if (_executing == null) {
        _executing = task;

        // ignore: unawaited_futures
        Future.microtask(() async {
          logger.logExecution('Executing task', this, task);
          await task.execute();
          assert(identical(_executing, task));
          _executing = null;
          _consumeQueue();
        });
      } else {
        _queue.add(task);
      }
    } else {
      // ignore: unawaited_futures
      Future.microtask(() {
        logger.logExecution('Executing task', this, task);
        return task.execute();
      });
    }

    return task._completer.future;
  }

  void _consumeQueue() {
    if (_queue.isNotEmpty) {
      var task = _queue.removeFirst();
      _executing = task;

      // ignore: unawaited_futures
      Future.microtask(() async {
        await task.execute();
        assert(identical(_executing, task));
        _executing = null;
        _consumeQueue();
      });
    }
  }

  @override
  String toString() {
    return '_AsyncExecutorSingleThread';
  }
}

/// Error for [AsyncTask] execution.
class AsyncExecutorError {
  /// The error.
  final dynamic cause;

  /// The error stack-trace.
  final dynamic stackTrace;

  AsyncExecutorError(this.cause, this.stackTrace);

  @override
  String toString() {
    return 'AsyncExecutorError{cause: $cause, stackTrace: $stackTrace}';
  }
}
