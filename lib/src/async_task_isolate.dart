import 'dart:async';
import 'dart:isolate';
import 'dart:math' as math;

import 'package:collection/collection.dart';

import 'async_task_base.dart';

class _TaskWrapper<P, R> {
  final AsyncTask task;

  final String taskType;

  final P parameters;

  final SharedData? sharedData;

  //final Completer<R> completer = Completer<R>();

  final DateTime submitTime = DateTime.now();

  DateTime? initTime;

  DateTime? endTime;

  _TaskWrapper(this.task)
      : taskType = task.taskType,
        parameters = task.parameters(),
        sharedData = task.sharedData() {
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
    int thID, DateTime submitTime, List<dynamic> message);

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

  Future<R> submit<P, R>(_TaskWrapper taskWrapper) async {
    lastCommunication = DateTime.now();

    var response = ReceivePort();

    var sharedData = taskWrapper.sharedData;

    List payload;
    if (sharedData != null) {
      payload = [
        taskWrapper.taskType,
        taskWrapper.parameters,
        sharedData.signature
      ];
    } else {
      payload = [taskWrapper.taskType, taskWrapper.parameters];
    }

    _sendPort!.send([
      response.sendPort,
      'msg',
      taskWrapper.submitTime.millisecondsSinceEpoch,
      payload
    ]);

    var ret = (await response.first) as List;

    // Isolate has requested the `SharedData`:
    if (ret[0] is String) {
      ret = await _submitSharedData(ret, sharedData);
    }

    lastCommunication = DateTime.now();

    var ok = ret[0] as bool;

    if (ok) {
      taskWrapper.initTime = DateTime.fromMillisecondsSinceEpoch(ret[1] as int);
      taskWrapper.endTime = DateTime.fromMillisecondsSinceEpoch(ret[2] as int);
      var result = ret[3];
      return result as R;
    } else {
      var error = ret[1];
      var stackTrace = ret[2];
      throw AsyncExecutorError(
          'Task execution error at $this', error, stackTrace);
    }
  }

  Future<List<dynamic>> _submitSharedData(
      List ret, SharedData? sharedData) async {
    // `SharedData` signature:
    var requestedSignature = ret[0];
    // The port to send the `SharedData`:
    var sharedDataReplyPort = ret[1] as SendPort;

    if (sharedData == null) {
      throw StateError(
          'Isolate requested a not present SharedData: $requestedSignature');
    }

    var signature = sharedData.signature;

    if (signature != requestedSignature) {
      throw StateError(
          'Different SharedData signature: requested = $requestedSignature ; signature: $signature');
    }

    _logger.logInfo('Sending SharedData[$signature] to $this');

    // The new port to send the task response:
    var response2 = ReceivePort();

    sharedDataReplyPort.send([
      sharedData.signature,
      sharedData.serializeCached(),
      response2.sendPort,
    ]);

    ret = (await response2.first) as List;
    return ret;
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
    if (message.length > 2) {
      String taskType = message[0];
      var sharedDataSign = message[2];

      var ret =
          await _resolveSharedData(thID, taskType, sharedDataSign, replyPort);

      var resolvedSharedData = ret.key;
      replyPort = ret.value;

      // Swap signature to `SharedData` instance:
      message[2] = resolvedSharedData;
    }

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

  static final Map<String, SharedData> _sharedDatas = <String, SharedData>{};
  static final Map<String, Completer<SharedData>> _requestingSharedDatas =
      <String, Completer<SharedData>>{};

  static Future<MapEntry<SharedData, SendPort>> _resolveSharedData(int thID,
      String taskType, String sharedDataSign, SendPort replyPort) async {
    var resolvedSharedData = _sharedDatas[sharedDataSign];

    // If `SharedData` with `sharedDataSign` is not present, request it:
    if (resolvedSharedData == null) {
      var requesting = _requestingSharedDatas[sharedDataSign];

      if (requesting != null) {
        var resolvedSharedData = await requesting.future;
        return MapEntry(resolvedSharedData, replyPort);
      }

      _requestingSharedDatas[sharedDataSign] =
          requesting = Completer<SharedData>();

      try {
        var taskRegistered = _IsolateThread._registeredTasks[taskType];
        if (taskRegistered == null) {
          throw StateError("Can't find registered task for: $taskType");
        }

        var sharedDataPort = ReceivePort();
        replyPort.send([sharedDataSign, sharedDataPort.sendPort]);

        var ret = (await sharedDataPort.first) as List;

        var retSign = ret[0] as String;
        var retData = ret[1];
        var retPort = ret[2] as SendPort;

        if (retSign != sharedDataSign) {
          throw StateError(
              'Different provided SharedData signature: requested = $sharedDataSign ; received = $retSign');
        }

        resolvedSharedData = taskRegistered.loadSharedData(retData)!;
        _sharedDatas[sharedDataSign] = resolvedSharedData;

        replyPort = retPort;

        requesting.complete(resolvedSharedData);

        return MapEntry(resolvedSharedData, replyPort);
      } catch (e, s) {
        requesting.completeError(e, s);
        rethrow;
      }
    }

    return MapEntry(resolvedSharedData, replyPort);
  }

  static Future<AsyncTask> _taskProcessor(
      int thID, DateTime submitTime, List<dynamic> message) async {
    String taskType = message[0];
    dynamic parameters = message[1];
    SharedData? sharedData = message.length > 2 ? message[2] : null;

    var taskRegistered = _IsolateThread._registeredTasks[taskType];

    if (taskRegistered == null) {
      throw StateError("Can't find registered task for: $taskType");
    }

    var task = taskRegistered.instantiate(parameters, sharedData);

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
