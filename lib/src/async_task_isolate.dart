import 'dart:async';
import 'dart:isolate';
import 'dart:math' as math;

import 'package:collection/collection.dart';

import 'async_task_base.dart';

class _TaskWrapper<P, R> {
  final AsyncTask task;

  final String taskType;

  final P parameters;

  //final Completer<R> completer = Completer<R>();

  final DateTime submitTime = DateTime.now();

  DateTime? initTime;

  DateTime? endTime;

  _TaskWrapper(this.task)
      : taskType = task.taskType,
        parameters = task.parameters() {
    task.submitTime = submitTime;
  }
}

class _AsyncExecutorMultiThread extends AsyncExecutorThread {
  static final List<_AsyncExecutorMultiThread> _instances =
      <_AsyncExecutorMultiThread>[];

  final AsyncTaskRegister taskRegister;
  final int totalThreads;

  final List<_IsolateThread> _threads = <_IsolateThread>[];
  final QueueList<_IsolateThread> _freeThreads = QueueList<_IsolateThread>();

  _AsyncExecutorMultiThread(AsyncTaskLoggerCaller logger, bool sequential,
      this.taskRegister, int totalThreads)
      : totalThreads = math.max(1, totalThreads),
        super(logger, sequential) {
    _instances.add(this);
  }

  @override
  Future<bool> start() async {
    logger.logInfo('Starting $this');
    if (_threads.isNotEmpty) {
      await Future.wait(_threads.map((t) => t.start()));
    }
    return true;
  }

  final QueueList<_TaskWrapper> _queue = QueueList<_TaskWrapper>(32);

  int _threadAsyncCount = -1;

  Future<_IsolateThread> _catchThread(String taskType) async {
    if (_freeThreads.isNotEmpty) {
      return _freeThreads.removeFirst();
    }

    if (_threads.length < totalThreads) {
      var newThread = _IsolateThread(taskRegister, logger);
      _threads.add(newThread);
      await newThread.start();
      return newThread;
    } else {
      var idx = (++_threadAsyncCount) % _threads.length;
      if (_threadAsyncCount > 10000000) {
        _threadAsyncCount = 0;
      }
      var thread = _threads[idx];
      await thread.waitStart();
      return thread;
    }
  }

  void _releaseThread(_IsolateThread thread) {
    _freeThreads.addFirst(thread);
  }

  @override
  Future<R> execute<P, R>(AsyncTask<P, R> task) async {
    var taskWrapper = _TaskWrapper<P, R>(task);

    if (sequential &&
        _threads.length >= totalThreads &&
        (_queue.isNotEmpty || _freeThreads.isEmpty)) {
      _queue.add(taskWrapper);
    } else {
      _dispatchTask(taskWrapper);
    }

    return task.waitResult();
  }

  void _dispatchTask<P, R>(_TaskWrapper<P, R> taskWrapper,
      [_IsolateThread? thread]) async {
    thread ??= await _catchThread(taskWrapper.taskType);

    try {
      logger.logExecution('Executing task', thread, taskWrapper.task);

      var result = await thread.submit(taskWrapper);
      finishTask(taskWrapper.task, taskWrapper.initTime!, taskWrapper.endTime!,
          result);

      logger.logExecution('Finished task', thread, taskWrapper.task);
    } catch (e, s) {
      finishTask(taskWrapper.task, taskWrapper.initTime, taskWrapper.endTime,
          null, e, s);

      logger.logExecution('Task error', thread, taskWrapper.task);
    } finally {
      if (_queue.isNotEmpty) {
        var task = _queue.removeFirst();
        // ignore: unawaited_futures
        Future.microtask(() => _dispatchTask(task, thread));
      } else {
        _releaseThread(thread);
      }
    }
  }

  @override
  String toString() {
    return '_AsyncExecutorMultiThread{ totalThreads: $totalThreads, queue: ${_queue.length} }';
  }
}

typedef _ThreadTaskProcessor = Future<AsyncTask> Function(
    int thID, DateTime submitTime, dynamic message);

class _IsolateThread {
  static int _idCounter = 0;

  final id;

  final AsyncTaskRegister _taskRegister;

  final AsyncTaskLoggerCaller _logger;

  _IsolateThread(this._taskRegister, this._logger) : id = ++_idCounter;

  List<String>? _registeredTasksTypes;

  List<String> get registeredTasksTypes => _registeredTasksTypes ?? <String>[];

  SendPort? _sendPort;

  bool _started = false;
  Completer<bool>? _starting;

  Future<bool> start() async {
    if (_started) return true;

    if (_starting != null) {
      return _starting!.future;
    }

    var starting = _starting = Completer<bool>();

    _logger.logInfo('Spawning Isolate for $this');

    var receivePort = ReceivePort();
    await Isolate.spawn(
        _process, [id, receivePort.sendPort, _taskRegister, _taskProcessor]);
    var ret = await receivePort.first;

    _sendPort = ret[0];
    _registeredTasksTypes = ret[1];

    _started = true;

    _logger.logInfo('Created ${toStringExtended()}');

    _waitStart.complete(true);
    starting.complete(true);

    return starting.future;
  }

  final Completer<bool> _waitStart = Completer<bool>();

  Future<bool> waitStart() {
    if (_waitStart.isCompleted) {
      return Future.value(true);
    } else {
      return _waitStart.future;
    }
  }

  Future<bool> close() async {
    var response = ReceivePort();
    _sendPort!.send([response.sendPort, 'close']);
    await response.first;
    return true;
  }

  DateTime lastCommunication = DateTime.now();

  Future<R> submit<P, R>(_TaskWrapper _basicTask) async {
    lastCommunication = DateTime.now();

    var response = ReceivePort();

    _sendPort!.send([
      response.sendPort,
      'msg',
      _basicTask.submitTime.millisecondsSinceEpoch,
      [_basicTask.taskType, _basicTask.parameters]
    ]);

    var ret = (await response.first) as List;

    lastCommunication = DateTime.now();

    var ok = ret[0] as bool;

    if (ok) {
      _basicTask.initTime = DateTime.fromMillisecondsSinceEpoch(ret[1] as int);
      _basicTask.endTime = DateTime.fromMillisecondsSinceEpoch(ret[2] as int);
      var result = ret[3];
      return result as R;
    } else {
      var error = ret[1];
      var stackTrace = ret[2];
      throw AsyncExecutorError(error, stackTrace);
    }
  }

  @override
  String toString() {
    return '_IsolateThread#$id';
  }

  String toStringExtended() {
    return '_IsolateThread{ id: $id ; registeredTasksTypes: $registeredTasksTypes }';
  }

  static final Map<String, AsyncTask> _registeredTasks = <String, AsyncTask>{};

  static void _process(List initMessage) async {
    var thID = initMessage[0];
    SendPort sendPort = initMessage[1];
    AsyncTaskRegister taskRegister = initMessage[2];
    _ThreadTaskProcessor taskProcessor = initMessage[3];

    var registeredTasks = await taskRegister();
    var registeredTasksTypes =
        registeredTasks.map((t) => t.taskType).toSet().toList();

    for (var task in registeredTasks) {
      var type = task.taskType;
      _registeredTasks[type] = task;
    }

    var port = ReceivePort();
    sendPort.send([port.sendPort, registeredTasksTypes]);

    await for (var p in port) {
      var payload = p as List;
      //print('PAYLOAD> $payload');

      var replyPort = payload[0] as SendPort;
      var type = payload[1] as String;

      switch (type) {
        case 'close':
          {
            port.close();
            replyPort.send(true);
            break;
          }
        case 'msg':
          {
            var submitTime =
                DateTime.fromMillisecondsSinceEpoch(payload[2] as int);
            var message = payload[3] as List;
            // ignore: unawaited_futures
            _processTask(thID, taskProcessor, submitTime, message, replyPort);
            break;
          }
        default:
          throw StateError("Can't handle payload: $payload");
      }
    }
  }

  static Future<void> _processTask(int thID, _ThreadTaskProcessor taskProcessor,
      DateTime submitTime, List<dynamic> message, SendPort replyPort) async {
    try {
      var task = await taskProcessor(thID, submitTime, message);
      var result = task.result;

      replyPort.send([
        true,
        task.initTime!.millisecondsSinceEpoch,
        task.endTime!.millisecondsSinceEpoch,
        result
      ]);
    } catch (e, s) {
      var lines = '$s'.split(RegExp(r'[\r\n]'));
      if (lines.last.isEmpty) {
        lines.removeLast();
      }

      replyPort.send([false, '$e', lines]);
    }
  }

  static Future<AsyncTask> _taskProcessor(
      int thID, DateTime submitTime, dynamic message) async {
    String taskType = message[0];
    dynamic parameters = message[1];

    var taskRegistered = _IsolateThread._registeredTasks[taskType];

    if (taskRegistered == null) {
      throw StateError("Can't find registered task for: $taskType");
    }

    var task = taskRegistered.instantiate(parameters);

    task.submitTime = submitTime;

    var ret = task.execute();

    if (ret is Future) {
      await ret;
    }

    return task;
  }
}

AsyncExecutorThread? createMultiThreadAsyncExecutorThread(
    AsyncTaskLoggerCaller logger, bool sequential, int parallelism,
    [AsyncTaskRegister? taskRegister]) {
  if (taskRegister == null) {
    throw StateError(
        'Multi-thread/isolate AsyncExecutor requires a "taskRegister" top-level function!');
  }

  return _AsyncExecutorMultiThread(
      logger, sequential, taskRegister, parallelism);
}
