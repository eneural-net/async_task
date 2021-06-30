import 'dart:async';
import 'dart:io';
import 'dart:isolate';
import 'dart:math' as math;

import 'package:collection/collection.dart';

import 'async_task_base.dart';
import 'async_task_channel.dart';
import 'async_task_extension.dart';
import 'async_task_shared_data.dart';

class _TaskWrapper<P, R> {
  final AsyncTask task;

  final String taskType;

  final P parameters;

  final Map<String, SharedData>? sharedData;

  final AsyncTaskChannel? taskChannel;

  final _AsyncTaskChannelPortIsolate? taskChannelPort;

  final AsyncExecutorSharedDataInfo? sharedDataInfo;

  final DateTime submitTime = DateTime.now();

  DateTime? initTime;

  DateTime? endTime;

  _TaskWrapper(this.task, this.taskChannel, this.sharedDataInfo)
      : taskType = task.taskType,
        taskChannelPort = taskChannel?.port as _AsyncTaskChannelPortIsolate?,
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

  static int getMaximumCores() {
    var cores = Platform.numberOfProcessors;
    return cores;
  }

  final AsyncTaskPlatform _platform =
      AsyncTaskPlatform(AsyncTaskPlatformType.isolate, getMaximumCores());

  @override
  int get maximumWorkers => totalThreads;

  @override
  AsyncTaskPlatform get platform => _platform;

  bool _start = false;

  @override
  FutureOr<bool> start() {
    if (_start) return true;
    _start = true;

    logger.logInfo('Starting $this');

    if (_threads.isNotEmpty) {
      var startFutures = <Future<_IsolateThread>>[];

      for (var thread in _threads) {
        var ret = thread._start();
        if (ret is Future) {
          startFutures.add(ret as Future<_IsolateThread>);
        }
      }

      if (startFutures.isNotEmpty) {
        return Future.wait(startFutures).then((_) => true);
      } else {
        return true;
      }
    } else {
      return true;
    }
  }

  @override
  bool isInExecutionContext(AsyncTask task) {
    if (task.executorThread != this) {
      throw StateError('Task not from this $this: $task');
    }

    return false;
  }

  final QueueList<_TaskWrapper> _queue = QueueList<_TaskWrapper>(32);

  int _threadAsyncCount = -1;

  FutureOr<_IsolateThread> _catchThread() {
    if (_freeThreads.isNotEmpty) {
      return _freeThreads.removeFirst();
    }

    return _catchNotFreeThread();
  }

  FutureOr<_IsolateThread> _catchNotFreeThread() {
    if (_threads.length < totalThreads && _closing == null) {
      var newThread = _IsolateThread(taskRegister, logger, _platform);
      _threads.add(newThread);
      return newThread._start();
    } else {
      var idx = (++_threadAsyncCount) % _threads.length;
      if (_threadAsyncCount > 10000000) {
        _threadAsyncCount = 0;
      }
      var thread = _threads[idx];
      return thread._waitStart();
    }
  }

  void _releaseThread(_IsolateThread thread) {
    _freeThreads.addFirst(thread);
  }

  final _RawReceivePortPool _receivePortPool = _RawReceivePortPool();

  int _executingTasks = 0;

  @override
  Future<R> execute<P, R>(AsyncTask<P, R> task,
      {AsyncExecutorSharedDataInfo? sharedDataInfo}) {
    if (_closed) {
      throw AsyncExecutorClosedError(null);
    }

    _executingTasks++;

    bindTask(task);

    var taskChannel = task.resolveChannel((t, c) {
      if (c != null) {
        c.initialize(
            t,
            _AsyncTaskChannelPortIsolate(
                c, _receivePortPool, _receivePortPool.catchPort(), false));
      }
    });

    var taskWrapper = _TaskWrapper<P, R>(task, taskChannel, sharedDataInfo);

    if (sequential &&
        _threads.length >= totalThreads &&
        (_queue.isNotEmpty || _freeThreads.isEmpty)) {
      _queue.add(taskWrapper);
    } else {
      _dispatchTask(taskWrapper);
    }

    task.waitResult().whenComplete(_onFinishTask);

    return task.waitResult();
  }

  void _onFinishTask() {
    --_executingTasks;
    assert(_executingTasks >= 0);

    if (_executingTasks == 0) {
      var waitingNoExecutingTasks = _waitingNoExecutingTasks;
      if (waitingNoExecutingTasks != null &&
          !waitingNoExecutingTasks.isCompleted) {
        waitingNoExecutingTasks.complete(true);
      }
    }
  }

  void _dispatchTask<P, R>(_TaskWrapper<P, R> taskWrapper) async {
    var thread = await _catchThread();
    thread.submit(taskWrapper, this);
  }

  void _dispatchTaskWithThread<P, R>(
      _TaskWrapper<P, R> taskWrapper, _IsolateThread thread) {
    thread.submit(taskWrapper, this);
  }

  void _onDispatchTaskFinished<P, R>(
    _TaskWrapper<P, R> taskWrapper,
    _IsolateThread thread,
    dynamic result, [
    dynamic error,
    StackTrace? stackTrace,
  ]) {
    try {
      if (error != null) {
        finishTask(taskWrapper.task, taskWrapper.initTime, taskWrapper.endTime,
            null, error, stackTrace);

        logger.logExecution('Task error', thread, taskWrapper.task);
      } else {
        finishTask(taskWrapper.task, taskWrapper.initTime!,
            taskWrapper.endTime!, result);

        logger.logExecution('Finished task', thread, taskWrapper.task);
      }
    } finally {
      if (_queue.isNotEmpty) {
        var task = _queue.removeFirst();

        Zone.current.scheduleMicrotask(() {
          _dispatchTaskWithThread(task, thread);
        });
      } else {
        _releaseThread(thread);
      }
    }
  }

  @override
  Future<bool> disposeSharedData<P, R>(Set<String> sharedDataSignatures,
      {bool async = false}) {
    if (sharedDataSignatures.isEmpty) return Future.value(true);

    if (async) {
      for (var t in _threads) {
        t._disposeSharedDataAsync(sharedDataSignatures);
      }
      return Future.value(true);
    } else {
      return Future.wait(
              _threads.map((e) => e._disposeSharedData(sharedDataSignatures)))
          .then((_) => true);
    }
  }

  @override
  Future<bool> disposeSharedDataInfo<P, R>(
      AsyncExecutorSharedDataInfo sharedDataInfo,
      {bool async = false}) {
    var sharedDataSignatures = sharedDataInfo.sentSharedDataSignatures;
    if (sharedDataSignatures.isEmpty) return Future.value(true);

    if (async) {
      for (var t in _threads) {
        t._disposeSharedDataAsync(sharedDataSignatures);
      }
      return Future.value(true);
    } else {
      return Future.wait(
              _threads.map((e) => e._disposeSharedData(sharedDataSignatures)))
          .then((_) {
        sharedDataInfo.disposedSharedDataSignatures
            .addAll(sharedDataSignatures);
        return true;
      });
    }
  }

  bool _closed = false;

  bool get closed => _closed;

  Completer<bool>? _closing;

  Completer<bool>? _waitingNoExecutingTasks;

  @override
  Future<bool> close() async {
    if (!_start) {
      await Future.delayed(Duration(seconds: 1));
    }

    if (_closed) return true;

    if (_closing != null) {
      return _closing!.future;
    }

    var closing = _closing = Completer<bool>();

    logger.logInfo('Closing $this ; Isolates: $_threads');

    if (_executingTasks > 0) {
      logger.logInfo(
          'Waiting for $_executingTasks tasks to execute before finishing isolates...');

      _waitingNoExecutingTasks = Completer<bool>();
      await _waitingNoExecutingTasks!.future;
    }

    await Future.wait(_threads.map((t) => t.close()));

    _threads.clear();

    _receivePortPool.close();

    _closed = true;
    closing.complete(true);

    logger.logInfo('Closed $this ; Isolates: $_threads');

    return closing.future;
  }

  @override
  String toString() {
    return '_AsyncExecutorMultiThread{ totalThreads: $totalThreads, queue: ${_queue.length} }';
  }
}

class _AsyncTaskChannelPortIsolate extends AsyncTaskChannelPort {
  final _RawReceivePortPool _receivePortPool;

  final _ReceivePort _receivePort;
  SendPort? _sendPort;

  final bool inExecutingContext;

  _AsyncTaskChannelPortIsolate(AsyncTaskChannel channel, this._receivePortPool,
      this._receivePort, this.inExecutingContext,
      [SendPort? sendPort])
      : super(channel) {
    if (sendPort == null) {
      _receivePort.setHandler(_onSendPortMessage);
    } else {
      _sendPort = sendPort;
      _receivePort.setHandler(_onPortMessage);
    }
  }

  _ReceivePort get receivePort => _receivePort;

  void _onSendPortMessage(message) {
    _sendPort = message;

    var unsetMessages = _unsetMessages;
    if (unsetMessages != null) {
      var sendPort = _sendPort!;
      for (var msg in unsetMessages) {
        sendPort.send(msg);
      }
      _unsetMessages = [];
    }

    _receivePort.setHandler(_onPortMessage);
  }

  void _onPortMessage(message) {
    onReceiveMessage(message, !inExecutingContext);
  }

  List<dynamic>? _unsetMessages;

  @override
  void send<M>(M message, bool inExecutingContext) {
    var sendPort = _sendPort;
    if (sendPort == null) {
      var unsetMessages = _unsetMessages ??= <dynamic>[];
      unsetMessages.add(message);
    } else {
      sendPort.send(message);
    }
  }

  @override
  bool isInExecutionContext(AsyncTask task) {
    return inExecutingContext;
  }

  @override
  void close() {
    super.close();
    _receivePortPool.releasePort(_receivePort);
  }

  @override
  String toString() {
    return '_AsyncTaskChannelPortIsolate{inExecutingContext: $inExecutingContext}';
  }
}

class _IsolateThread {
  static int _idCounter = 0;

  final id;

  final AsyncTaskRegister _taskRegister;

  final AsyncTaskLoggerCaller _logger;

  final AsyncTaskPlatform _platform;

  final _RawReceivePortPool _receivePortPool = _RawReceivePortPool();

  _IsolateThread(this._taskRegister, this._logger, this._platform)
      : id = ++_idCounter;

  List<String>? _registeredTasksTypes;

  List<String> get registeredTasksTypes => _registeredTasksTypes ?? <String>[];

  SendPort? _sendPort;

  bool _started = false;
  Completer<_IsolateThread>? _starting;

  FutureOr<_IsolateThread> _start() {
    if (_started) return this;

    if (_starting != null) {
      return _starting!.future;
    }

    return _startImpl();
  }

  Future<_IsolateThread> _startImpl() async {
    var starting = _starting = Completer<_IsolateThread>();

    _logger.logInfo('Spawning Isolate for $this');

    var completer = Completer<List>();
    var receivePort =
        _receivePortPool.catchPortWithCompleterAndAutoRelease(completer);

    await Isolate.spawn(_Isolate._asyncTaskExecutor_Isolate_main,
        [id, receivePort.sendPort, _taskRegister]);
    var ret = await completer.future;

    _sendPort = ret[0];
    _registeredTasksTypes = ret[1];

    _started = true;

    _logger.logInfo('Created ${toStringExtended()}');

    _waitStartCompleter.complete(this);
    starting.complete(this);

    return this;
  }

  final Completer<_IsolateThread> _waitStartCompleter =
      Completer<_IsolateThread>();

  FutureOr<_IsolateThread> _waitStart() {
    if (_waitStartCompleter.isCompleted) {
      return this;
    } else {
      return _waitStartCompleter.future;
    }
  }

  Future<bool> close() async {
    var completer = Completer<bool>();
    var receivePort =
        _receivePortPool.catchPortWithCompleterAndAutoRelease(completer);

    _sendPort!.send([receivePort.sendPort, 'close']);
    await completer.future;

    _receivePortPool.close();

    return true;
  }

  DateTime lastCommunication = DateTime.now();

  final Set<String> _sentSharedDataSignatures = <String>{};

  void submit<P, R>(
      _TaskWrapper taskWrapper, _AsyncExecutorMultiThread parent) {
    lastCommunication = DateTime.now();

    parent.logger.logExecution('Executing task', this, taskWrapper.task);

    var responsePort = _receivePortPool.catchPort();
    responsePort.setHandler(
        (ret) => _onSubmitResponse(taskWrapper, parent, responsePort, ret));

    var sharedData = taskWrapper.sharedData;

    if (sharedData != null) {
      var sentSharedData = 0;

      var sharedDataMap = sharedData.map((k, sd) {
        var signature = sd.signature;
        if (_sentSharedDataSignatures.contains(signature)) {
          return MapEntry(k, signature);
        } else {
          taskWrapper.sharedDataInfo?.sentSharedDataSignatures.add(signature);
          _sentSharedDataSignatures.add(sd.signature);

          sentSharedData++;

          _logger.logExecution('Pre-sending SharedData:', signature, this);

          return MapEntry(
              k,
              _SharedDataSerial(
                  signature, sd.serializeCached(platform: parent.platform)));
        }
      });

      var sentAllSharedData = sentSharedData == sharedDataMap.length;

      _sendPort!.send([
        responsePort.sendPort,
        'task',
        taskWrapper.taskType,
        taskWrapper.submitTime.millisecondsSinceEpoch,
        taskWrapper.taskChannelPort?.receivePort.sendPort,
        taskWrapper.parameters,
        sentAllSharedData,
        sharedDataMap
      ]);
    } else {
      _sendPort!.send([
        responsePort.sendPort,
        'task',
        taskWrapper.taskType,
        taskWrapper.submitTime.millisecondsSinceEpoch,
        taskWrapper.taskChannelPort?.receivePort.sendPort,
        taskWrapper.parameters
      ]);
    }
  }

  void _onSubmitResponse<P, R>(_TaskWrapper<P, R> taskWrapper,
      _AsyncExecutorMultiThread parent, _ReceivePort responsePort, List ret) {
    if (ret[0] is String) {
      _submitSharedData(ret, responsePort, taskWrapper.sharedData,
          taskWrapper.sharedDataInfo);
    } else {
      _receivePortPool.releasePort(responsePort);

      var ok = ret[0] as bool;

      if (ok) {
        taskWrapper.initTime =
            DateTime.fromMillisecondsSinceEpoch(ret[1] as int);
        taskWrapper.endTime =
            DateTime.fromMillisecondsSinceEpoch(ret[2] as int);
        var result = ret[3];

        parent._onDispatchTaskFinished(taskWrapper, this, result as R);
      } else {
        var error = ret[1];
        var stackTrace = ret[2];

        var error2 = AsyncExecutorError(
            'Task execution error at $this', error, stackTrace);

        parent._onDispatchTaskFinished(
            taskWrapper, this, null, error2, StackTrace.current);
      }
    }
  }

  void _submitSharedData(
      List ret,
      _ReceivePort responsePort,
      Map<String, SharedData>? sharedDataMap,
      AsyncExecutorSharedDataInfo? sharedDataInfo) {
    var requestedKey = ret[0];
    // `SharedData` signature:
    var requestedSignature = ret[1];
    // The port to send the `SharedData`:
    var sharedDataReplyPort = ret[2] as SendPort;

    if (sharedDataMap == null) {
      throw StateError(
          'Isolate requested from a not present SharedData Map: $requestedKey = $requestedSignature');
    }

    var sharedData = sharedDataMap[requestedKey];

    if (sharedData == null) {
      throw StateError(
          'Isolate requested a not present SharedData: $requestedKey = $requestedSignature');
    }

    var signature = sharedData.signature;

    if (signature != requestedSignature) {
      throw StateError(
          'Different SharedData signature: requested = $requestedSignature ; signature: $signature');
    }

    sharedDataInfo?.sentSharedDataSignatures.add(signature);
    _sentSharedDataSignatures.add(signature);

    _logger.logExecution('Sending SharedData', signature, this);

    sharedDataReplyPort.send([
      requestedKey,
      sharedData.signature,
      sharedData.serializeCached(platform: _platform),
      responsePort.sendPort,
    ]);
  }

  Future<bool> _disposeSharedData<P, R>(Set<String> sharedDataSignatures) {
    var completer = Completer<bool>();

    _sentSharedDataSignatures.removeAll(sharedDataSignatures);

    var response =
        _receivePortPool.catchPortWithCompleterAndAutoRelease(completer);

    _sendPort!
        .send([response.sendPort, 'disposeSharedData', sharedDataSignatures]);

    return completer.future;
  }

  void _disposeSharedDataAsync<P, R>(Set<String> sharedDataSignatures) {
    _sentSharedDataSignatures.removeAll(sharedDataSignatures);

    var response = _receivePortPool.catchPortWithAutoRelease();

    _sendPort!
        .send([response.sendPort, 'disposeSharedData', sharedDataSignatures]);
  }

  @override
  String toString() {
    return '_IsolateThread#$id';
  }

  String toStringExtended() {
    return '_IsolateThread{ id: $id ; registeredTasksTypes: $registeredTasksTypes }';
  }
}

class _Isolate {
  // Use an extended name for the Isolate entrypoint to be easily visible
  // in the Observatory and debugging tools.
  static void _asyncTaskExecutor_Isolate_main(List initMessage) async {
    var thID = initMessage[0];
    SendPort sendPort = initMessage[1];
    AsyncTaskRegister taskRegister = initMessage[2];

    var isolate = _Isolate(thID);

    await isolate._start(sendPort, taskRegister);
  }

  final int thID;

  final Map<String, AsyncTask> _registeredTasks = <String, AsyncTask>{};

  final RawReceivePort _port = RawReceivePort();

  final _RawReceivePortPool _receivePortPool = _RawReceivePortPool();

  _Isolate(this.thID) {
    _port.handler = _onMessage;
  }

  Future<void> _start(SendPort sendPort, AsyncTaskRegister taskRegister) async {
    var registeredTasks = await taskRegister();
    var registeredTasksTypes =
        registeredTasks.map((t) => t.taskType).toSet().toList();

    for (var task in registeredTasks) {
      var type = task.taskType;
      _registeredTasks[type] = task;
    }

    sendPort.send([_port.sendPort, registeredTasksTypes]);
  }

  void _onMessage(List message) {
    var replyPort = message[0] as SendPort;
    var type = message[1] as String;

    switch (type) {
      case 'close':
        {
          _onClose();
          replyPort.send(true);
          Zone.current.scheduleMicrotask(() {
            _port.close();
            Isolate.current.kill();
          });
          break;
        }
      case 'task':
        {
          var taskType = message[2] as String;
          var submitTime =
              DateTime.fromMillisecondsSinceEpoch(message[3] as int);
          var taskChannelSendPort = message[4] as SendPort?;

          _ReceivePort? taskChannelReceivePort;
          if (taskChannelSendPort != null) {
            taskChannelReceivePort = _receivePortPool.catchPort();
            taskChannelSendPort.send(taskChannelReceivePort.sendPort);
          }

          var parameters = message[5];

          _processTask(thID, taskType, submitTime, taskChannelReceivePort,
              taskChannelSendPort, parameters, message, replyPort);
          break;
        }
      case 'disposeSharedData':
        {
          var sharedDataSignatures = message[2] as Set<String>;
          _sharedDatas.removeAllKeys(sharedDataSignatures);
          replyPort.send(true);
          break;
        }
      default:
        throw StateError("Can't handle payload: $message");
    }
  }

  void _onClose() {
    _port.close();
    _receivePortPool.close();
  }

  final Map<String, SharedData> _sharedDatas = <String, SharedData>{};

  void _processTask(
      int thID,
      String taskType,
      DateTime submitTime,
      _ReceivePort? taskChannelReceivePort,
      SendPort? taskChannelSendPort,
      dynamic parameters,
      List message,
      SendPort replyPort) {
    if (message.length > 6) {
      var sentAllSharedData = message[6] as bool;
      var sharedDataMap = message[7] as Map<String, Object>;

      if (sentAllSharedData) {
        _processTask_withPreSentSharedData(
          thID,
          taskType,
          submitTime,
          taskChannelReceivePort,
          taskChannelSendPort,
          parameters,
          sharedDataMap,
          replyPort,
        );
      } else {
        _processTask_withSharedDataToResolve(
          thID,
          taskType,
          submitTime,
          taskChannelReceivePort,
          taskChannelSendPort,
          parameters,
          sharedDataMap,
          replyPort,
        );
      }
    } else {
      _processTask_execute(thID, taskType, submitTime, taskChannelReceivePort,
          taskChannelSendPort, parameters, null, replyPort);
    }
  }

  void _processTask_withPreSentSharedData(
      int thID,
      String taskType,
      DateTime submitTime,
      _ReceivePort? taskChannelReceivePort,
      SendPort? taskChannelSendPort,
      dynamic parameters,
      Map<String, Object> sharedDataMap,
      SendPort replyPort) {
    var taskRegistered = _getRegisteredTask(taskType);

    var sharedDataMapResolved = sharedDataMap.map((k, v) {
      var sharedDataSerial = (v as _SharedDataSerial);
      var sharedData =
          taskRegistered.loadSharedData(k, sharedDataSerial.serial)!;
      _sharedDatas[sharedDataSerial.signature] = sharedData;
      return MapEntry(k, sharedData);
    });

    _processTask_execute(thID, taskType, submitTime, taskChannelReceivePort,
        taskChannelSendPort, parameters, sharedDataMapResolved, replyPort);
  }

  AsyncTask<dynamic, dynamic> _getRegisteredTask(String taskType) {
    var taskRegistered = _registeredTasks[taskType];
    if (taskRegistered == null) {
      throw StateError("Can't find registered task for: $taskType");
    }
    return taskRegistered;
  }

  void _processTask_withSharedDataToResolve(
      int thID,
      String taskType,
      DateTime submitTime,
      _ReceivePort? taskChannelReceivePort,
      SendPort? taskChannelSendPort,
      dynamic parameters,
      Map<String, Object> sharedDataMap,
      SendPort replyPort) {
    var needToRequestSharedData = false;
    for (var v in sharedDataMap.values) {
      if (v is String && !_sharedDatas.containsKey(v)) {
        needToRequestSharedData = true;
      }
    }

    if (needToRequestSharedData) {
      _processTask_resolveRemoteSharedData(
          thID,
          taskType,
          submitTime,
          taskChannelReceivePort,
          taskChannelSendPort,
          parameters,
          sharedDataMap,
          replyPort);
    } else {
      _processTask_resolveLocalSharedData(
          thID,
          taskType,
          submitTime,
          taskChannelReceivePort,
          taskChannelSendPort,
          parameters,
          sharedDataMap,
          replyPort);
    }
  }

  void _processTask_resolveLocalSharedData(
      int thID,
      String taskType,
      DateTime submitTime,
      _ReceivePort? taskChannelReceivePort,
      SendPort? taskChannelSendPort,
      dynamic parameters,
      Map<String, Object> sharedDataMap,
      SendPort replyPort) {
    var taskRegistered = _getRegisteredTask(taskType);

    var sharedDataMapResolved = sharedDataMap.map((k, v) {
      if (v is _SharedDataSerial) {
        var sharedData = taskRegistered.loadSharedData(k, v.serial)!;
        _sharedDatas[v.signature] = sharedData;
        return MapEntry(k, sharedData);
      } else if (v is String) {
        var sharedData = _sharedDatas[v]!;
        return MapEntry(k, sharedData);
      } else {
        throw StateError('Invalid value: $v');
      }
    });

    _processTask_execute(thID, taskType, submitTime, taskChannelReceivePort,
        taskChannelSendPort, parameters, sharedDataMapResolved, replyPort);
  }

  void _processTask_resolveRemoteSharedData(
      int thID,
      String taskType,
      DateTime submitTime,
      _ReceivePort? taskChannelReceivePort,
      SendPort? taskChannelSendPort,
      dynamic parameters,
      Map<String, Object> sharedDataMap,
      SendPort replyPort) async {
    var ret =
        await _resolveSharedDataMap(thID, taskType, sharedDataMap, replyPort);

    var sharedDataMapResolved = ret.key;
    replyPort = ret.value;

    _processTask_execute(thID, taskType, submitTime, taskChannelReceivePort,
        taskChannelSendPort, parameters, sharedDataMapResolved, replyPort);
  }

  void _processTask_execute(
      int thID,
      String taskType,
      DateTime submitTime,
      _ReceivePort? taskChannelReceivePort,
      SendPort? taskChannelSendPort,
      dynamic parameters,
      Map<String, SharedData>? sharedDataMap,
      SendPort replyPort) {
    AsyncTask? instantiatedTask;
    try {
      var taskRegistered = _getRegisteredTask(taskType);
      var task = instantiatedTask =
          taskRegistered.instantiate(parameters, sharedDataMap);

      task.submitTime = submitTime;

      task.resolveChannel((t, c) {
        if (c != null) {
          c.initialize(
              t,
              _AsyncTaskChannelPortIsolate(c, _receivePortPool,
                  taskChannelReceivePort!, true, taskChannelSendPort));
        }
      });

      var ret = task.execute();

      if (ret is Future) {
        // ignore: unawaited_futures
        ret.then((_) {
          var result = task.result;
          _processTask_replyResult(task, result, replyPort);
        }, onError: (e, s) {
          _processTask_replyError(task, e, s, replyPort);
        });
      } else {
        _processTask_replyResult(task, ret, replyPort);
      }
    } catch (e, s) {
      _processTask_replyError(instantiatedTask, e, s, replyPort);
    }
  }

  void _processTask_replyResult(
      AsyncTask<dynamic, dynamic> task, dynamic result, SendPort replyPort) {
    replyPort.send([
      true,
      task.initTime!.millisecondsSinceEpoch,
      task.endTime!.millisecondsSinceEpoch,
      result
    ]);

    var taskChannel = task.channelResolved();

    if (taskChannel != null) {
      taskChannel.close();
    }
  }

  void _processTask_replyError(AsyncTask<dynamic, dynamic>? task, Object error,
      StackTrace s, SendPort replyPort) {
    var lines = '$s'.split(RegExp(r'[\r\n]'));
    if (lines.last.isEmpty) {
      lines.removeLast();
    }

    replyPort.send([false, '$error', lines]);

    if (task != null) {
      var taskChannel = task.channelResolved();

      if (taskChannel != null) {
        taskChannel.close();
      }
    }
  }

  final Map<String, Completer<SharedData>> _requestingSharedDatas =
      <String, Completer<SharedData>>{};

  Future<MapEntry<Map<String, SharedData>, SendPort>> _resolveSharedDataMap(
      int thID,
      String taskType,
      Map<String, Object> sharedDataMap,
      SendPort replyPort) async {
    var taskRegistered = _getRegisteredTask(taskType);

    var resolvedMap = <String, SharedData>{};

    for (var entry in sharedDataMap.entries) {
      var key = entry.key;
      var val = entry.value;

      if (val is _SharedDataSerial) {
        var resolvedSharedData =
            taskRegistered.loadSharedData(key, val.serial)!;

        _sharedDatas[val.signature] = resolvedSharedData;

        resolvedMap[key] = resolvedSharedData;
      } else if (val is String) {
        var ret =
            await _resolveSharedData(thID, taskRegistered, key, val, replyPort);

        resolvedMap[key] = ret.key;
        replyPort = ret.value;
      } else {
        throw StateError('Invalid value: $val');
      }
    }

    return MapEntry(resolvedMap, replyPort);
  }

  Future<MapEntry<SharedData, SendPort>> _resolveSharedData(
      int thID,
      AsyncTask taskRegistered,
      String sharedDataKey,
      String sharedDataSign,
      SendPort replyPort) async {
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
        var completer = Completer<List>();
        var sharedDataPort =
            _receivePortPool.catchPortWithCompleterAndAutoRelease(completer);

        replyPort
            .send([sharedDataKey, sharedDataSign, sharedDataPort.sendPort]);
        var ret = await completer.future;

        var retKey = ret[0] as String;
        var retSign = ret[1] as String;
        var retSerial = ret[2];
        var retPort = ret[3] as SendPort;

        if (retKey != sharedDataKey) {
          throw StateError(
              'Different provided SharedData key: requested = $sharedDataKey ; received = $retKey');
        }

        if (retSign != sharedDataSign) {
          throw StateError(
              'Different provided SharedData signature: requested = $sharedDataSign ; received = $retSign');
        }

        resolvedSharedData =
            taskRegistered.loadSharedData(sharedDataKey, retSerial)!;
        _sharedDatas[sharedDataSign] = resolvedSharedData;

        replyPort = retPort;

        requesting.complete(resolvedSharedData);

        _requestingSharedDatas.remove(sharedDataSign);

        return MapEntry(resolvedSharedData, replyPort);
      } catch (e, s) {
        requesting.completeError(e, s);
        rethrow;
      }
    }

    return MapEntry(resolvedSharedData, replyPort);
  }
}

class _SharedDataSerial<S> {
  final String signature;
  final S serial;

  _SharedDataSerial(this.signature, this.serial);
}

class _RawReceivePortPool {
  final QueueList<_ReceivePort> _pool = QueueList<_ReceivePort>(32);

  _ReceivePort catchPort() {
    if (_pool.isNotEmpty) {
      return _pool.removeLast();
    }
    return _ReceivePort();
  }

  _ReceivePort catchPortWithHandler(PortHandler handler) {
    var port = catchPort();
    port.setHandler(handler);
    return port;
  }

  _ReceivePort catchPortWithAutoRelease<R>() {
    var port = catchPort();
    port.setHandler((r) {
      releasePort(port);
    });
    return port;
  }

  _ReceivePort catchPortWithCompleterAndAutoRelease<R>(Completer<R> completer) {
    var port = catchPort();
    port.setHandler((r) {
      releasePort(port);
      completer.complete(r);
    });

    return port;
  }

  _ReceivePort catchPortWithCompleterCallbackAndAutoRelease<R>(
      Completer<R> completer, void Function(R ret) callback) {
    var port = catchPort();
    port.setHandler((r) {
      releasePort(port);
      callback(r);
      completer.complete(r);
    });
    return port;
  }

  void releasePort(_ReceivePort port) {
    port.disposeHandler();
    assert(!_pool.contains(port));
    _pool.addLast(port);
  }

  void close() {
    for (var port in _pool) {
      port.close();
    }
  }
}

void _unsetHandler(dynamic message) =>
    throw StateError('_ReceivePort.handler called while unset!');

typedef PortHandler = void Function(dynamic message);

class _ReceivePort {
  final RawReceivePort _port = RawReceivePort();

  PortHandler _handler = _unsetHandler;

  _ReceivePort() {
    _port.handler = _callHandler;
  }

  SendPort get sendPort => _port.sendPort;

  void setHandler<T>(PortHandler handler) {
    _handler = handler;
  }

  void disposeHandler() {
    _handler = _unsetHandler;
  }

  void _callHandler(dynamic message) {
    _handler(message);
  }

  void close() => _port.close();
}

int? _getAsyncExecutorMaximumParallelism;

int getAsyncExecutorMaximumParallelism() =>
    _getAsyncExecutorMaximumParallelism ??= Platform.numberOfProcessors;

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
