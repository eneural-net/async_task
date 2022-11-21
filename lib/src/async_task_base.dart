import 'package:async_extension/async_extension.dart';
import 'package:collection/collection.dart';

import 'async_task_channel.dart';
import 'async_task_generic.dart'
    if (dart.library.isolate) 'async_task_isolate.dart';
import 'async_task_shared_data.dart';

/// Status of an [AsyncTask].
enum AsyncTaskStatus {
  idle,
  executing,
  successful,
  error,
}

/// [AsyncTask] Platform type.
enum AsyncTaskPlatformType {
  generic,
  isolate,
}

/// [AsyncTask] Platform information.
class AsyncTaskPlatform {
  final AsyncTaskPlatformType platformType;

  /// Maximum number of threads/isolates of the platform.
  final int maximumParallelism;

  AsyncTaskPlatform(this.platformType, this.maximumParallelism);

  /// Returns `true` if is a native platform.
  bool get isNative => platformType == AsyncTaskPlatformType.isolate;

  /// Returns `true` if is a generic platform.
  bool get isGeneric => platformType == AsyncTaskPlatformType.generic;

  @override
  String toString() {
    return 'AsyncTaskPlatform{platformType: $platformType, maximumParallelism: $maximumParallelism}';
  }
}

typedef OnFinishAsyncTask = void Function(
    AsyncTask asyncTask, Object? result, Object? error, StackTrace? stackTrace);

/// Base class for tasks implementation.
///
/// - [P] is the [parameters] type.
/// - [R] is the [result] type.
abstract class AsyncTask<P, R> {
  final Completer<R> _completer = Completer<R>();

  String? _taskType;

  String get taskType {
    var taskType = _taskType;
    if (taskType == null) {
      _taskType = taskType = '$runtimeType';
    }
    return taskType;
  }

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
  FutureOr<R> execute() {
    _initTime = DateTime.now();

    submitTime ??= _initTime;

    try {
      var ret = run();

      if (ret is Future) {
        var future = ret as Future;

        return future.then((result) {
          _finish(result, endTime: DateTime.now());
          return result;
        }, onError: (e, s) {
          _finishError(e, s, endTime: DateTime.now());
        });
      } else {
        _finish(ret, endTime: DateTime.now());
        return ret;
      }
    } catch (e, s) {
      _finishError(e, s, endTime: DateTime.now());
      rethrow;
    }
  }

  List<OnFinishAsyncTask>? _onFinishAsyncTaskTriggers;

  /// Adds a trigger to be called when this tasks finishes.
  ///
  /// - Throws a [StateError] if is already finished ([isFinished]).
  void addOnFinishAsyncTask(OnFinishAsyncTask onFinishAsyncTask) {
    if (isFinished) {
      throw StateError('Task already finished');
    }

    var triggers = _onFinishAsyncTaskTriggers ??= <OnFinishAsyncTask>[];
    triggers.add(onFinishAsyncTask);
  }

  void _callOnFinishAsyncTask(
      Object? result, Object? error, StackTrace? stackTrace) {
    var triggers = _onFinishAsyncTaskTriggers;
    if (triggers != null) {
      _onFinishAsyncTaskTriggers = null;
      for (var trigger in triggers) {
        try {
          trigger(this, result, error, stackTrace);
        } catch (e, s) {
          print(e);
          _executorThread?.logger.logError(error, s);
        }
      }
    }
  }

  void _finish(R result, {DateTime? initTime, DateTime? endTime}) {
    _setExecutionTime(initTime, endTime);
    _result = result;
    _finished = true;
    _callOnFinishAsyncTask(result, null, null);
    _completer.complete(result);
    _finishChannel();
  }

  void _finishError(Object error, StackTrace? stackTrace,
      {DateTime? initTime, DateTime? endTime}) {
    _setExecutionTime(initTime, endTime);
    _error = error;
    _finished = true;
    _callOnFinishAsyncTask(null, error, stackTrace);
    _completer.completeError(error, stackTrace);
    _finishChannel();
  }

  void _finishChannel() {
    var channel = _channelInstance;
    if (channel != null) {
      channel.close();
    }
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

  /// Returns an optional [Map] of [SharedData], where each entry will be
  /// transferred to the executor thread/isolate only once.
  Map<String, SharedData>? sharedData() => null;

  /// Load of a `SharedData` [serial] for the corresponding [key].
  SharedData? loadSharedData(String key, dynamic serial) => null;

  /// The parameters of this tasks.
  ///
  /// - NOTE: [P] should be a type that can be sent through an [Isolate] or
  ///   serialized as JSON.
  P parameters();

  /// Creates an instance of this task type with [parameters] and optional [sharedData].
  AsyncTask<P, R> instantiate(P parameters,
      [Map<String, SharedData>? sharedData]);

  /// Creates a copy of this task in its initial state (before execution state).
  AsyncTask<P, R> copy() => instantiate(parameters(), sharedData());

  /// Computes/runs this task.
  FutureOr<R> run();

  AsyncExecutorThread? _executorThread;

  AsyncExecutorThread? get executorThread => _executorThread;

  /// if `true`, indicates that the current context is inside [AsyncTask.run].
  bool get isInExecutionContext {
    var executorThread = _executorThread;
    return executorThread != null
        ? executorThread.isInExecutionContext(this)
        : false;
  }

  AsyncTaskChannel? _channelInstance;

  bool _channelResolved = false;

  Completer<AsyncTaskChannel?>? _waitingChannelToResolve;

  /// Returns an optional [AsyncTaskChannel] for communication with the task.
  FutureOr<AsyncTaskChannel?> channel() {
    if (_channelResolved) return _channelInstance;
    var waitingChannelToResolve = _waitingChannelToResolve;
    waitingChannelToResolve ??= _waitingChannelToResolve = Completer();
    return waitingChannelToResolve.future;
  }

  /// Returns an optional [AsyncTaskChannel]. If not resolved yet will return null.
  AsyncTaskChannel? channelResolved() {
    if (_channelResolved) return _channelInstance;
    return null;
  }

  /// Resolves the channel instance. Called by [AsyncExecutor] when executing this task.
  AsyncTaskChannel? resolveChannel(
      void Function(AsyncTask task, AsyncTaskChannel channel) initializer) {
    if (_channelResolved) return _channelInstance;
    var channel = _channelInstance = channelInstantiator();

    if (channel != null) {
      initializer(this, channel);
    }

    _channelResolved = true;
    var waitingChannelToResolve = _waitingChannelToResolve;
    if (waitingChannelToResolve != null) {
      waitingChannelToResolve.complete(channel);
    }

    return channel;
  }

  /// Instantiate the optional [AsyncTaskChannel] return by [channel].
  AsyncTaskChannel? channelInstantiator() => null;

  /// Resets this task to it's initial state, before any execution.
  void reset() {
    _finished = false;
    _error = null;
    submitTime = null;
    _initTime = null;
    _endTime = null;
    _result = null;
  }

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
    if (s.length > limit) {
      s = '${s.substring(0, limit)}...';
    }
    return s;
  }
}

AsyncExecutorThread createAsyncExecutorThread(String executorName,
    AsyncTaskLoggerCaller logger, bool sequential, int parallelism,
    [AsyncTaskRegister? taskRegister]) {
  if (parallelism >= 1) {
    return (createMultiThreadAsyncExecutorThread(
            executorName, logger, sequential, parallelism, taskRegister) ??
        _AsyncExecutorSingleThread(executorName, logger, sequential));
  } else {
    return _AsyncExecutorSingleThread(executorName, logger, sequential);
  }
}

typedef AsyncTaskLogger = void Function(String type, dynamic message,
    [dynamic error, dynamic stackTrace]);

void defaultAsyncTaskLogger(String type, dynamic message,
    [dynamic error, dynamic stackTrace]) {
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
}

class AsyncTaskLoggerCaller {
  final AsyncTaskLogger _logger;

  AsyncTaskLoggerCaller(AsyncTaskLogger? logger)
      : _logger = logger ?? defaultAsyncTaskLogger;

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

  void logExecution(dynamic message, dynamic a, dynamic b) {
    if (enabledExecution) {
      log('EXEC', '$message > $a > $b');
    }
  }
}

typedef AsyncTaskRegister = FutureOr<List<AsyncTask>> Function();

enum AsyncExecutorStatus {
  idle,
  started,
  ready,
  closing,
  closed,
}

/// Asynchronous Executor of [AsyncTask].
class AsyncExecutor {
  /// The maximum number of threads/isolates that this process can have.
  static int get maximumParallelism => getAsyncExecutorMaximumParallelism();

  /// Returns a valid [parallelism] parameter to use in constructor.
  static int parameterParallelism({int? value, double? byPercentage}) {
    if (byPercentage != null) {
      if (byPercentage > 1) byPercentage /= 100;
      var p = (maximumParallelism * byPercentage).round();
      return p.clamp(0, maximumParallelism);
    } else if (value != null) {
      return value.clamp(0, maximumParallelism);
    } else {
      return 2.clamp(0, maximumParallelism);
    }
  }

  /// The name of this executor (for debug purposes).
  final String name;

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
      {this.name = '',
      this.sequential = false,
      int? parallelism,
      double? parallelismPercentage,
      this.taskTypeRegister,
      AsyncTaskLogger? logger})
      : parallelism = parameterParallelism(
            value: parallelism, byPercentage: parallelismPercentage),
        _executorThread = createAsyncExecutorThread(
            name,
            AsyncTaskLoggerCaller(logger),
            sequential,
            parameterParallelism(
                value: parallelism, byPercentage: parallelismPercentage),
            taskTypeRegister) {
    _logger = _executorThread.logger;
  }

  AsyncTaskPlatform get platform => _executorThread.platform;

  int get maximumWorkers => _executorThread.maximumWorkers;

  AsyncExecutorThreadInfo get info => _executorThread.info;

  AsyncTaskLoggerCaller get logger => _logger;

  bool _started = false;

  bool get isStarted => _started;

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

    _executorThread.start().then((_) {
      _started = true;
      starting.complete(true);
      _flushTasksToExecute();
      _starting = null;
    });

    return starting.future;
  }

  /// Execute [task] using this executor.
  ///
  /// If task was already submitted the execution will be skipped.
  Future<R> execute<P, R>(AsyncTask<P, R> task,
      {AsyncExecutorSharedDataInfo? sharedDataInfo}) {
    if (task.wasSubmitted) {
      return task.waitResult();
    }

    if (_closed) {
      throw AsyncExecutorClosedError('Not ready: executor closed.');
    } else if (_closing != null) {
      logger.logWarn('Executing task while closing executor: $task');
    }

    if (!_started) {
      return _executeNotStarted(task, sharedDataInfo);
    } else {
      return _executeAlreadyStarted(task, sharedDataInfo);
    }
  }

  List<_TaskToExecute>? _tasksToExecuteQueue;

  Future<R> _executeNotStarted<P, R>(
      AsyncTask<P, R> task, AsyncExecutorSharedDataInfo? sharedDataInfo) {
    var tasksToExecuteQueue = _tasksToExecuteQueue ??= <_TaskToExecute>[];
    var taskToExecute = _TaskToExecute<P, R>(task, sharedDataInfo);
    tasksToExecuteQueue.add(taskToExecute);

    start();

    return taskToExecute.completer.future;
  }

  void _flushTasksToExecute() {
    var tasksToExecuteQueue = _tasksToExecuteQueue;
    if (tasksToExecuteQueue == null) return;
    _tasksToExecuteQueue = null;

    for (var t in tasksToExecuteQueue) {
      t.execute(this);
    }
  }

  Future<R> _executeAlreadyStarted<P, R>(
      AsyncTask<P, R> task, AsyncExecutorSharedDataInfo? sharedDataInfo) {
    if (_tasksToExecuteQueue != null) {
      _flushTasksToExecute();
    }

    return _executorThread.execute(task, sharedDataInfo: sharedDataInfo);
  }

  /// Disposes [SharedData] sent to other `threads/isolates`.
  ///
  /// - [sharedDataSignatures] the signatures of [SharedData] to dispose.
  FutureOr<bool> disposeSharedData<P, R>(Set<String> sharedDataSignatures,
          {bool async = false}) =>
      _executorThread.disposeSharedData(sharedDataSignatures, async: async);

  /// Disposes [SharedData] sent to other `threads/isolates`.
  ///
  /// - [sharedDataInfo] the [AsyncExecutorSharedDataInfo] with sent [SharedData] signatures.
  ///   Note that [AsyncExecutorSharedDataInfo.disposeSharedDataInfo] will be populated.
  FutureOr<bool> disposeSharedDataInfo<P, R>(
          AsyncExecutorSharedDataInfo sharedDataInfo,
          {bool async = false}) =>
      _executorThread.disposeSharedDataInfo(sharedDataInfo, async: async);

  /// Executes all the [tasks] using this executor. Calling [execute]
  /// for each task.
  List<Future<R>> executeAll<P, R>(Iterable<AsyncTask<P, R>> tasks,
      {AsyncExecutorSharedDataInfo? sharedDataInfo}) {
    if (tasks is List<AsyncTask<P, R>>) {
      return List.generate(tasks.length,
          (i) => execute(tasks[i], sharedDataInfo: sharedDataInfo));
    } else {
      return tasks
          .toList()
          .map((t) => execute(t, sharedDataInfo: sharedDataInfo))
          .toList();
    }
  }

  /// Executes all the [tasks] using this executor and waits for the results.
  Future<List<R>> executeAllAndWaitResults<P, R>(
      Iterable<AsyncTask<P, R>> tasks,
      {AsyncExecutorSharedDataInfo? sharedDataInfo,
      bool disposeSentSharedData = false}) async {
    if (disposeSentSharedData) {
      sharedDataInfo ??= AsyncExecutorSharedDataInfo();

      var executions = executeAll(tasks, sharedDataInfo: sharedDataInfo);
      var results = await Future.wait(executions);

      if (sharedDataInfo.sentSharedDataSignatures.isNotEmpty) {
        await disposeSharedData(sharedDataInfo.sentSharedDataSignatures);
        sharedDataInfo.disposedSharedDataSignatures
            .addAll(sharedDataInfo.sentSharedDataSignatures);
      }

      return results;
    } else {
      var executions = executeAll(tasks, sharedDataInfo: sharedDataInfo);
      return Future.wait(executions);
    }
  }

  bool _closed = false;

  /// Returns `true` if this executor is already closed.
  bool get isClosed => _closed;

  Completer<bool>? _closing;

  /// Returns `true` if this executor is closing.
  bool get isClosing => _closing != null;

  /// Returns `true` if this executor is ready to receive tasks to execute.
  bool get isReady => _started && !isClosed && !isClosing;

  /// Closes this executor.
  Future<bool> close() async {
    if (_closed) return true;

    if (_closing != null) {
      return _closing!.future;
    }

    var closing = _closing = Completer<bool>();

    await _executorThread.close();

    _closed = true;
    closing.complete(true);

    return closing.future;
  }

  AsyncExecutorStatus get status {
    if (isReady) {
      return AsyncExecutorStatus.ready;
    } else if (isClosed) {
      return AsyncExecutorStatus.closed;
    } else if (isClosing) {
      return AsyncExecutorStatus.closing;
    } else if (isStarted) {
      return AsyncExecutorStatus.started;
    } else {
      return AsyncExecutorStatus.idle;
    }
  }

  @override
  String toString() {
    return 'AsyncExecutor{'
        ' sequential: $sequential,'
        ' parallelism: $parallelism,'
        ' maximumWorkers: $maximumWorkers,'
        ' status: $status,'
        ' platform: $platform,'
        ' executorThread: $_executorThread'
        ' }';
  }
}

/// This is a task in a execution queue, of not dispatched tasks.
///
/// Used when [AsyncExecutor.execute] is called before [AsyncExecutor.start].
class _TaskToExecute<P, R> {
  final AsyncTask<P, R> task;

  final AsyncExecutorSharedDataInfo? sharedDataInfo;

  _TaskToExecute(this.task, this.sharedDataInfo);

  Completer<R> get completer => task._completer;

  void execute(AsyncExecutor executor) =>
      executor.execute(task, sharedDataInfo: sharedDataInfo);
}

/// An [AsyncExecutorThread] info.
class AsyncExecutorThreadInfo {
  /// `true` if the [AsyncExecutor] is sequencial.
  final bool sequential;

  /// The maximum number of workers of the [AsyncExecutor].
  final int maximumWorkers;

  /// Threads information.
  final List<AsyncThreadInfo> threads;

  /// `true` if the [AsyncExecutor] really runs in parallel (separated `Isolate`, `Thread` or `Worker`).
  final bool parallel;

  AsyncExecutorThreadInfo(
      this.sequential, this.maximumWorkers, this.threads, this.parallel);

  /// Total number of threads/workers.
  int get workers => threads.length;

  @override
  String toString() {
    return 'AsyncExecutorThreadInfo{ sequential: $sequential, maximumWorkers: $maximumWorkers, workers: $workers, parallel: $parallel }';
  }
}

/// A thread info.
class AsyncThreadInfo {
  /// The ID of the thread/worker.
  final int id;

  /// The total dispatched tasks by the thread/worker.
  final int dispatchedTasks;

  /// The total executed tasks by the thread/worker.
  final int executedTasks;

  AsyncThreadInfo(this.id, this.dispatchedTasks, this.executedTasks);

  /// The current executing tasks in the thread/worker.
  int get executingTasks => dispatchedTasks - executedTasks;

  @override
  String toString() {
    return 'AsyncThreadInfo{ id: $id, dispatchedTasks: $dispatchedTasks, executedTasks: $executedTasks, executingTasks: $executingTasks }';
  }
}

/// Base class for executor thread implementation.
abstract class AsyncExecutorThread {
  final String executorName;
  final AsyncTaskLoggerCaller logger;
  final bool sequential;

  AsyncExecutorThread(this.executorName, this.logger, this.sequential);

  AsyncTaskPlatform get platform;

  int get maximumWorkers;

  AsyncExecutorThreadInfo get info;

  /// Starts this thread.
  FutureOr<bool> start();

  void bindTask(AsyncTask task) {
    task._executorThread = this;
  }

  /// if `true`, indicates that the current context is inside [AsyncTask.run].
  bool isInExecutionContext(AsyncTask task);

  /// Executes [task] in this thread.
  Future<R> execute<P, R>(AsyncTask<P, R> task,
      {AsyncExecutorSharedDataInfo? sharedDataInfo});

  /// Disposes [SharedData] sent to other `threads/isolates`.
  ///
  /// - [sharedDataSignatures] the signatures of [SharedData] to dispose.
  FutureOr<bool> disposeSharedData<P, R>(Set<String> sharedDataSignatures,
      {bool async = false});

  /// Disposes [SharedData] sent to other `threads/isolates`.
  ///
  /// - [sharedDataInfo] the [AsyncExecutorSharedDataInfo] with sent [SharedData] signatures.
  ///   Note that [AsyncExecutorSharedDataInfo.disposeSharedDataInfo] will be populated.
  FutureOr<bool> disposeSharedDataInfo<P, R>(
      AsyncExecutorSharedDataInfo sharedDataInfo,
      {bool async = false});

  /// Perform a task finish operation.
  void finishTask<P, R>(
      AsyncTask<P, R> task, DateTime? initTime, DateTime? endTime, R result,
      [Object? error, StackTrace? stackTrace]) {
    if (!task.isFinished) {
      if (error != null) {
        task._finishError(error, stackTrace,
            initTime: initTime, endTime: endTime);
      } else {
        task._finish(result, initTime: initTime, endTime: endTime);
      }
    }
  }

  /// Closes this instance.
  Future<bool> close() async {
    return true;
  }
}

class _AsyncExecutorSingleThread extends AsyncExecutorThread {
  _AsyncExecutorSingleThread(
      String executorName, AsyncTaskLoggerCaller logger, bool sequential)
      : super(executorName, logger, sequential);

  final AsyncTaskPlatform _platform =
      AsyncTaskPlatform(AsyncTaskPlatformType.generic, 1);

  @override
  AsyncTaskPlatform get platform => _platform;

  @override
  int get maximumWorkers => 1;

  @override
  AsyncExecutorThreadInfo get info => AsyncExecutorThreadInfo(
      sequential,
      maximumWorkers,
      [AsyncThreadInfo(0, _dispatchedTasksCount, _executedTasksCount)],
      false);

  Zone? _zone;

  bool _started = false;

  @override
  FutureOr<bool> start() {
    if (_started) return true;
    _started = true;

    _zone = Zone.current.fork();
    return true;
  }

  @override
  bool isInExecutionContext(AsyncTask task) {
    if (task.executorThread != this) {
      throw StateError('Task not from this $this: $task');
    }

    var myZone = _zone;
    if (myZone == null) return false;

    var currentZone = Zone.current;
    var executingContext = identical(myZone, currentZone);

    return executingContext;
  }

  int _dispatchedTasksCount = 0;
  int _executedTasksCount = 0;

  final QueueList<AsyncTask> _sequentialQueue = QueueList<AsyncTask>(32);
  AsyncTask? _executing;

  @override
  Future<R> execute<P, R>(AsyncTask<P, R> task,
      {AsyncExecutorSharedDataInfo? sharedDataInfo}) {
    bindTask(task);

    task.resolveChannel(
        (t, c) => c.initialize(t, _AsyncTaskChannelPortLocal(c)));

    if (sequential) {
      if (_executing == null) {
        _executing = task;

        _zone!.scheduleMicrotask(() async {
          logger.logExecution('Executing task', this, task);
          ++_dispatchedTasksCount;

          task.addOnFinishAsyncTask((asyncTask, result, error, stackTrace) {
            assert(identical(_executing, asyncTask));

            _executing = null;
            ++_executedTasksCount;
            _consumeSequentialQueue();
          });

          task.execute();
        });
      } else {
        _sequentialQueue.add(task);
      }
    } else {
      _zone!.scheduleMicrotask(() {
        logger.logExecution('Executing task', this, task);
        ++_dispatchedTasksCount;

        task.addOnFinishAsyncTask(
            (asyncTask, result, error, stackTrace) => ++_executedTasksCount);

        task.execute();
      });
    }

    return task._completer.future;
  }

  void _consumeSequentialQueue() {
    if (_sequentialQueue.isEmpty) return;

    var task = _sequentialQueue.removeFirst();
    _executing = task;

    Zone.current.scheduleMicrotask(() {
      logger.logExecution('Executing task', this, task);
      ++_dispatchedTasksCount;

      task.addOnFinishAsyncTask((asyncTask, result, error, stackTrace) {
        assert(identical(_executing, asyncTask));

        ++_executedTasksCount;
        _executing = null;
        _consumeSequentialQueue();
      });

      task.execute();
    });
  }

  @override
  bool disposeSharedData<P, R>(Set<String> sharedDataSignatures,
          {bool async = false}) =>
      true;

  @override
  bool disposeSharedDataInfo<P, R>(AsyncExecutorSharedDataInfo sharedDataInfo,
          {bool async = false}) =>
      true;

  @override
  String toString() {
    return '_AsyncExecutorSingleThread';
  }
}

class _AsyncTaskChannelPortLocal extends AsyncTaskChannelPort {
  _AsyncTaskChannelPortLocal(AsyncTaskChannel channel) : super(channel);

  @override
  void send<M>(M message, bool inExecutingContext) {
    onReceiveMessage(message, inExecutingContext);
  }
}

class AsyncExecutorClosedError extends AsyncExecutorError {
  AsyncExecutorClosedError(dynamic cause) : super(cause);
}

/// Error for [AsyncTask] execution.
class AsyncExecutorError {
  final String? message;

  /// The error.
  final dynamic cause;

  /// The error stack-trace.
  final dynamic stackTrace;

  AsyncExecutorError(this.message, [this.cause, this.stackTrace]);

  @override
  String toString() {
    return 'AsyncExecutorError{message: $message, cause: $cause, stackTrace: $stackTrace}';
  }
}
