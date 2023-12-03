import 'dart:io';
import 'dart:isolate';
import 'dart:math' as math;

import 'package:async_extension/async_extension.dart';
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

  _AsyncExecutorMultiThread(super.executorName, super.logger, super.sequential,
      this.taskRegister, int totalThreads)
      : totalThreads = math.max(1, totalThreads) {
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
  AsyncExecutorThreadInfo get info => AsyncExecutorThreadInfo(
      sequential, maximumWorkers, _threads.map((t) => t.info).toList(), true);

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
      var newThread =
          _IsolateThread(executorName, taskRegister, logger, _platform);
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

    var taskChannel = task.resolveChannel((t, c) => c.initialize(
        t,
        _AsyncTaskChannelPortIsolate(
            c, _receivePortPool, _receivePortPool.catchPort(), false)));

    task.addOnFinishAsyncTask(
        (asyncTask, result, error, stackTrace) => _onFinishTask());

    var taskWrapper = _TaskWrapper<P, R>(task, taskChannel, sharedDataInfo);

    if (sequential &&
        _threads.length >= totalThreads &&
        (_queue.isNotEmpty || _freeThreads.isEmpty)) {
      _queue.add(taskWrapper);
    } else {
      _dispatchTask(taskWrapper);
    }

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

  void _dispatchTask<P, R>(_TaskWrapper<P, R> taskWrapper) {
    var thread = _catchThread();

    if (thread is Future<_IsolateThread>) {
      thread.then((t) => t.submit(taskWrapper, this));
    } else {
      thread.submit(taskWrapper, this);
    }
  }

  void _dispatchTaskWithThread<P, R>(
      _TaskWrapper<P, R> taskWrapper, _IsolateThread thread) {
    thread.submit(taskWrapper, this);
  }

  void _onDispatchTaskFinished<P, R>(
    _TaskWrapper<P, R> taskWrapper,
    _IsolateThread thread,
    Object? result, [
    Object? error,
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
  FutureOr<bool> disposeSharedData<P, R>(Set<String> sharedDataSignatures,
      {bool async = false}) {
    if (sharedDataSignatures.isEmpty) return true;

    if (async) {
      for (var t in _threads) {
        t._disposeSharedDataAsync(sharedDataSignatures);
      }
      return true;
    } else {
      return Future.wait(
              _threads.map((e) => e._disposeSharedData(sharedDataSignatures)))
          .then((_) => true);
    }
  }

  @override
  FutureOr<bool> disposeSharedDataInfo<P, R>(
      AsyncExecutorSharedDataInfo sharedDataInfo,
      {bool async = false}) {
    var sharedDataSignatures = sharedDataInfo.sentSharedDataSignatures;
    if (sharedDataSignatures.isEmpty) return true;

    if (async) {
      for (var t in _threads) {
        t._disposeSharedDataAsync(sharedDataSignatures);
      }
      return true;
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

  _AsyncTaskChannelPortIsolate(super.channel, this._receivePortPool,
      this._receivePort, this.inExecutingContext,
      [SendPort? sendPort]) {
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

  List<Object?>? _unsetMessages;

  @override
  void send<M>(M message, bool inExecutingContext) {
    var sendPort = _sendPort;
    if (sendPort == null) {
      var unsetMessages = _unsetMessages ??= <Object?>[];
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
    if (isClosed) return;
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

  final int id = ++_idCounter;

  final String executorName;

  final AsyncTaskRegister _taskRegister;

  final AsyncTaskLoggerCaller _logger;

  final AsyncTaskPlatform _platform;

  final _RawReceivePortPool _receivePortPool = _RawReceivePortPool();

  _IsolateThread(
      this.executorName, this._taskRegister, this._logger, this._platform);

  AsyncThreadInfo get info =>
      AsyncThreadInfo(id, _submittedTasks, _executedTasks);

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

    await Isolate.spawn(_Isolate._asyncTaskExecutorIsolateMain,
        [id, receivePort.sendPort, _taskRegister],
        debugName: 'async_task/_IsolateThread/$id');

    var ret = await completer.future
        .timeout(Duration(seconds: 15), onTimeout: () => []);

    if (ret.isEmpty) {
      throw TimeoutException(
          'Isolate#$id for `AsyncExecutor{name: "$executorName"}` start not completed!');
    }

    _sendPort = ret[0];
    _registeredTasksTypes = ret[1];

    _started = true;

    _logger.logInfo('Created ${toStringExtended()}');

    _waitStartCompleter.complete(this);
    starting.complete(this);

    _starting = null;

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

  bool _closed = false;

  bool get isClosed => _closed;

  Future<bool> close() async {
    var completer = Completer<bool>();
    var receivePort =
        _receivePortPool.catchPortWithCompleterAndAutoRelease(completer);

    _sendPort!.send(
      _IsolateMsgClose(receivePort.sendPort),
    );

    await completer.future;

    _receivePortPool.close();

    _closed = true;

    return true;
  }

  DateTime lastCommunication = DateTime.now();

  final Set<String> _sentSharedDataSignatures = <String>{};

  int _submittedTasks = 0;
  int _executedTasks = 0;

  void submit<P, R>(
      _TaskWrapper taskWrapper, _AsyncExecutorMultiThread parent) {
    lastCommunication = DateTime.now();

    parent.logger.logExecution('Executing task', this, taskWrapper.task);

    ++_submittedTasks;

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

          var sharedDataSerial = _SharedDataSerial(
              signature, sd.serializeCached(platform: parent.platform));

          return MapEntry(k, sharedDataSerial);
        }
      });

      sharedDataMap = Map<String, Object>.unmodifiable(sharedDataMap);

      var sentAllSharedData = sentSharedData == sharedDataMap.length;

      _sendPort!.send(
        _IsolateMsgTask(
            responsePort.sendPort,
            taskWrapper.taskType,
            taskWrapper.submitTime,
            taskWrapper.taskChannelPort?.receivePort.sendPort,
            taskWrapper.parameters,
            sentAllSharedData,
            sharedDataMap),
      );
    } else {
      _sendPort!.send(
        _IsolateMsgTask(
          responsePort.sendPort,
          taskWrapper.taskType,
          taskWrapper.submitTime,
          taskWrapper.taskChannelPort?.receivePort.sendPort,
          taskWrapper.parameters,
        ),
      );
    }
  }

  void _onSubmitResponse<P, R>(
      _TaskWrapper<P, R> taskWrapper,
      _AsyncExecutorMultiThread parent,
      _ReceivePort responsePort,
      _IsolateReply ret) {
    if (ret is _IsolateReplySharedDataRequest) {
      _submitSharedData(ret, responsePort, taskWrapper.sharedData,
          taskWrapper.sharedDataInfo);
    } else if (ret is _IsolateReplyResult) {
      _receivePortPool.releasePort(responsePort);
      ++_executedTasks;

      taskWrapper.initTime = ret.initTime;
      taskWrapper.endTime = ret.endTime;
      var result = ret.result;

      parent._onDispatchTaskFinished(taskWrapper, this, result as R);
    } else if (ret is _IsolateReplyError) {
      _receivePortPool.releasePort(responsePort);
      ++_executedTasks;

      var error = ret.error;
      var stackTrace = ret.stackTrace;

      var error2 = AsyncExecutorError(
          'Task execution error at $this', error, stackTrace);

      parent._onDispatchTaskFinished(
          taskWrapper, this, null, error2, StackTrace.current);
    } else {
      throw StateError("Unknown reply: $ret");
    }
  }

  void _submitSharedData(
      _IsolateReplySharedDataRequest req,
      _ReceivePort responsePort,
      Map<String, SharedData>? sharedDataMap,
      AsyncExecutorSharedDataInfo? sharedDataInfo) {
    var requestedKey = req.key;
    // `SharedData` signature:
    var requestedSignature = req.sharedDataSignature;
    // The port to send the `SharedData`:
    var sharedDataReplyPort = req.replyPort;

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

    sharedDataReplyPort.send(
      _IsolateReplySharedData(
          requestedKey,
          sharedData.signature,
          sharedData.serializeCached(platform: _platform),
          responsePort.sendPort),
    );
  }

  Future<bool> _disposeSharedData<P, R>(Set<String> sharedDataSignatures) {
    var completer = Completer<bool>();

    _sentSharedDataSignatures.removeAll(sharedDataSignatures);

    var response =
        _receivePortPool.catchPortWithCompleterAndAutoRelease(completer);

    _sendPort!.send(
      _IsolateMsgDisposeSharedData(response.sendPort, sharedDataSignatures),
    );

    return completer.future;
  }

  void _disposeSharedDataAsync<P, R>(Set<String> sharedDataSignatures) {
    _sentSharedDataSignatures.removeAll(sharedDataSignatures);

    var response = _receivePortPool.catchPortWithAutoRelease();

    _sendPort!.send(
      _IsolateMsgDisposeSharedData(response.sendPort, sharedDataSignatures),
    );
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
  static void _asyncTaskExecutorIsolateMain(List initMessage) {
    runZonedGuarded(() {
      var thID = initMessage[0];
      SendPort sendPort = initMessage[1];
      AsyncTaskRegister taskRegister = initMessage[2];

      var isolate = _Isolate(thID);

      isolate._start(sendPort, taskRegister);
    }, (e, s) {
      print('** AsyncExecutor Isolate error: $e');
    });
  }

  final int thID;

  final Map<String, AsyncTask> _registeredTasks = <String, AsyncTask>{};

  final RawReceivePort _port = RawReceivePort();

  final _RawReceivePortPool _receivePortPool = _RawReceivePortPool();

  _Isolate(this.thID) {
    _port.handler = _onMessage;
  }

  void _start(SendPort sendPort, AsyncTaskRegister taskRegister) {
    var registeredTasks = taskRegister();
    if (registeredTasks is Future<List<AsyncTask>>) {
      registeredTasks.then((tasks) {
        _startImpl(tasks, sendPort);
      });
    } else {
      _startImpl(registeredTasks, sendPort);
    }
  }

  void _startImpl(List<AsyncTask> registeredTasks, SendPort sendPort) {
    for (var task in registeredTasks) {
      _registeredTasks[task.taskType] = task;
    }

    var registeredTasksTypes =
        registeredTasks.map((t) => t.taskType).toSet().toList();

    sendPort.send(
      [_port.sendPort, registeredTasksTypes],
    );
  }

  void _onMessage(_IsolateMsg msg) {
    var replyPort = msg.replyPort;

    switch (msg.type) {
      case _IsolateMsgType.close:
        {
          _onClose();
          replyPort.send(true);

          Zone.current.scheduleMicrotask(() {
            _receivePortPool.close();
            _port.close();
            Isolate.current.kill();
          });

          break;
        }
      case _IsolateMsgType.task:
        {
          var message = msg as _IsolateMsgTask;

          var taskChannelSendPort = message.taskChannelSendPort;

          _ReceivePort? taskChannelReceivePort;
          if (taskChannelSendPort != null) {
            taskChannelReceivePort = _receivePortPool.catchPort();
            taskChannelSendPort.send(taskChannelReceivePort.sendPort);
          }

          _processTask(thID, message, taskChannelReceivePort,
              taskChannelSendPort, replyPort);

          break;
        }
      case _IsolateMsgType.disposeSharedData:
        {
          var message = msg as _IsolateMsgDisposeSharedData;
          _sharedDatas.removeAllKeys(message.sharedDataSignatures);
          replyPort.send(true);
          break;
        }
      default:
        throw StateError("Can't handle message: $msg");
    }
  }

  void _onClose() {
    _port.close();
    _receivePortPool.close();
  }

  final Map<String, SharedData> _sharedDatas = <String, SharedData>{};

  void _processTask(
      int thID,
      _IsolateMsgTask message,
      _ReceivePort? taskChannelReceivePort,
      SendPort? taskChannelSendPort,
      SendPort replyPort) {
    if (message.sentAllSharedData != null) {
      var sentAllSharedData = message.sentAllSharedData!;
      var sharedDataMap = message.sharedDataMap!;

      if (sentAllSharedData) {
        _processTaskWithPreSentSharedData(
          thID,
          message.taskType,
          message.submitTime,
          taskChannelReceivePort,
          taskChannelSendPort,
          message.parameters,
          sharedDataMap,
          replyPort,
        );
      } else {
        _processTaskWithSharedDataToResolve(
          thID,
          message.taskType,
          message.submitTime,
          taskChannelReceivePort,
          taskChannelSendPort,
          message.parameters,
          sharedDataMap,
          replyPort,
        );
      }
    } else {
      _processTaskExecute(
        thID,
        message.taskType,
        message.submitTime,
        taskChannelReceivePort,
        taskChannelSendPort,
        message.parameters,
        null,
        replyPort,
      );
    }
  }

  void _processTaskWithPreSentSharedData(
      int thID,
      String taskType,
      DateTime submitTime,
      _ReceivePort? taskChannelReceivePort,
      SendPort? taskChannelSendPort,
      Object? parameters,
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

    _processTaskExecute(thID, taskType, submitTime, taskChannelReceivePort,
        taskChannelSendPort, parameters, sharedDataMapResolved, replyPort);
  }

  AsyncTask _getRegisteredTask(String taskType) {
    var taskRegistered = _registeredTasks[taskType];
    if (taskRegistered == null) {
      throw StateError("Can't find registered task for: $taskType");
    }
    return taskRegistered;
  }

  void _processTaskWithSharedDataToResolve(
      int thID,
      String taskType,
      DateTime submitTime,
      _ReceivePort? taskChannelReceivePort,
      SendPort? taskChannelSendPort,
      Object? parameters,
      Map<String, Object> sharedDataMap,
      SendPort replyPort) {
    var needToRequestSharedData = false;
    for (var v in sharedDataMap.values) {
      if (v is String && !_sharedDatas.containsKey(v)) {
        needToRequestSharedData = true;
      }
    }

    if (needToRequestSharedData) {
      _processTaskResolveRemoteSharedData(
          thID,
          taskType,
          submitTime,
          taskChannelReceivePort,
          taskChannelSendPort,
          parameters,
          sharedDataMap,
          replyPort);
    } else {
      _processTaskResolveLocalSharedData(
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

  void _processTaskResolveLocalSharedData(
      int thID,
      String taskType,
      DateTime submitTime,
      _ReceivePort? taskChannelReceivePort,
      SendPort? taskChannelSendPort,
      Object? parameters,
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

    _processTaskExecute(thID, taskType, submitTime, taskChannelReceivePort,
        taskChannelSendPort, parameters, sharedDataMapResolved, replyPort);
  }

  void _processTaskResolveRemoteSharedData(
      int thID,
      String taskType,
      DateTime submitTime,
      _ReceivePort? taskChannelReceivePort,
      SendPort? taskChannelSendPort,
      Object? parameters,
      Map<String, Object> sharedDataMap,
      SendPort replyPort) async {
    var ret =
        await _resolveSharedDataMap(thID, taskType, sharedDataMap, replyPort);

    var sharedDataMapResolved = ret.key;
    replyPort = ret.value;

    _processTaskExecute(thID, taskType, submitTime, taskChannelReceivePort,
        taskChannelSendPort, parameters, sharedDataMapResolved, replyPort);
  }

  void _processTaskExecute(
      int thID,
      String taskType,
      DateTime submitTime,
      _ReceivePort? taskChannelReceivePort,
      SendPort? taskChannelSendPort,
      Object? parameters,
      Map<String, SharedData>? sharedDataMap,
      SendPort replyPort) {
    AsyncTask? instantiatedTask;

    try {
      var taskRegistered = _getRegisteredTask(taskType);
      var task = instantiatedTask =
          taskRegistered.instantiate(parameters, sharedDataMap);

      task.submitTime = submitTime;

      task.resolveChannel((t, c) => c.initialize(
          t,
          _AsyncTaskChannelPortIsolate(c, _receivePortPool,
              taskChannelReceivePort!, true, taskChannelSendPort)));

      task.addOnFinishAsyncTask((asyncTask, result, error, stackTrace) {
        if (error != null) {
          _processTaskReplyError(asyncTask, error, stackTrace, replyPort);
        } else {
          _processTaskReplyResult(asyncTask, result, replyPort);
        }
      });

      task.execute();
    } catch (e, s) {
      _processTaskReplyError(instantiatedTask, e, s, replyPort);
    }
  }

  void _processTaskReplyResult(
      AsyncTask task, Object? result, SendPort replyPort) {
    replyPort.send(
      _IsolateReplyResult(task.initTime!, task.endTime!, result),
    );
  }

  void _processTaskReplyError(
      AsyncTask? task, Object error, StackTrace? s, SendPort replyPort) {
    var lines = '$s'.split(RegExp(r'[\r\n]'));
    if (lines.last.isEmpty) {
      lines.removeLast();
    }

    replyPort.send(
      _IsolateReplyError('$error', lines),
    );
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

    // If `SharedData` with `sharedDataSign` is already present:
    if (resolvedSharedData != null) {
      return MapEntry(resolvedSharedData, replyPort);
    }

    // Check if the `SharedData` with `sharedDataSign` is being requested:

    var requesting = _requestingSharedDatas[sharedDataSign];

    if (requesting != null) {
      var resolvedSharedData = await requesting.future;
      return MapEntry(resolvedSharedData, replyPort);
    }

    // Request the missing `SharedData`:

    _requestingSharedDatas[sharedDataSign] =
        requesting = Completer<SharedData>();

    try {
      var completer = Completer<_IsolateReplySharedData>();
      var sharedDataPort =
          _receivePortPool.catchPortWithCompleterAndAutoRelease(completer);

      replyPort.send(
        _IsolateReplySharedDataRequest(
            sharedDataKey, sharedDataSign, sharedDataPort.sendPort),
      );

      var ret = await completer.future;

      var retKey = ret.key;
      var retSign = ret.sharedDataSignature;
      var retSerial = ret.sharedDataSerial;
      var retPort = ret.replyPort;

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
}

enum _IsolateMsgType { close, task, disposeSharedData }

sealed class _IsolateMsg {
  final _IsolateMsgType type;
  final SendPort replyPort;

  const _IsolateMsg(this.type, this.replyPort);
}

final class _IsolateMsgClose extends _IsolateMsg {
  const _IsolateMsgClose(SendPort replyPort)
      : super(_IsolateMsgType.close, replyPort);
}

final class _IsolateMsgTask extends _IsolateMsg {
  final String taskType;

  final DateTime submitTime;

  final SendPort? taskChannelSendPort;

  final Object parameters;

  final bool? sentAllSharedData;
  final Map<String, Object>? sharedDataMap;

  const _IsolateMsgTask(SendPort replyPort, this.taskType, this.submitTime,
      this.taskChannelSendPort, this.parameters,
      [this.sentAllSharedData, this.sharedDataMap])
      : super(_IsolateMsgType.task, replyPort);
}

final class _IsolateMsgDisposeSharedData extends _IsolateMsg {
  final Set<String> sharedDataSignatures;

  const _IsolateMsgDisposeSharedData(
      SendPort replyPort, this.sharedDataSignatures)
      : super(_IsolateMsgType.disposeSharedData, replyPort);
}

sealed class _IsolateReply {
  const _IsolateReply();
}

final class _IsolateReplySharedDataRequest extends _IsolateReply {
  final String key;
  final String sharedDataSignature;
  final SendPort replyPort;

  const _IsolateReplySharedDataRequest(
      this.key, this.sharedDataSignature, this.replyPort);
}

final class _IsolateReplySharedData extends _IsolateReply {
  final String key;
  final String sharedDataSignature;
  final Object sharedDataSerial;
  final SendPort replyPort;

  const _IsolateReplySharedData(this.key, this.sharedDataSignature,
      this.sharedDataSerial, this.replyPort);
}

final class _IsolateReplyResult extends _IsolateReply {
  final DateTime initTime;
  final DateTime endTime;
  final Object? result;

  const _IsolateReplyResult(this.initTime, this.endTime, this.result);
}

final class _IsolateReplyError extends _IsolateReply {
  final Object? error;
  final List<String>? stackTrace;

  const _IsolateReplyError(this.error, this.stackTrace);
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

void _unsetHandler(Object? message) =>
    throw StateError('_ReceivePort.handler called while unset!');

void _disableHandler(Object? message) {}

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
    _handler = _disableHandler;
  }

  void _callHandler(dynamic message) {
    if (!_closed) {
      _handler(message);
    }
  }

  bool _closed = false;

  void close() {
    if (_closed) return;
    _closed = true;
    _port.close();
    disposeHandler();
  }
}

int? _getAsyncExecutorMaximumParallelism;

int getAsyncExecutorMaximumParallelism() =>
    _getAsyncExecutorMaximumParallelism ??= Platform.numberOfProcessors;

AsyncExecutorThread? createMultiThreadAsyncExecutorThread(String executorName,
    AsyncTaskLoggerCaller logger, bool sequential, int parallelism,
    [AsyncTaskRegister? taskRegister]) {
  if (taskRegister == null) {
    throw StateError(
        'Multi-thread/isolate AsyncExecutor requires a "taskRegister" top-level function!');
  }

  return _AsyncExecutorMultiThread(
      executorName, logger, sequential, taskRegister, parallelism);
}
