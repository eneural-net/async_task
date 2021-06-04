import 'dart:async';
import 'dart:math' as math;
import 'dart:typed_data';

import 'package:collection/collection.dart';

import 'async_task_extension.dart';
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
    if (s.length > limit) s = s.substring(0, limit) + '...';
    return s;
  }
}

/// Interface for serializable data classes:
abstract class SerializableData<D, S> {
  /// Creates a new instance with [data].
  SerializableData<D, S> instantiate(D data);

  /// Creates a copy of this instance.
  SerializableData<D, S> copy();

  /// Deserializes [serial] to [D].
  D deserializeData(S serial);

  /// Deserializes [serial] to a instance of this type.
  SerializableData<D, S> deserialize(S serial) =>
      instantiate(deserializeData(serial));

  /// Serializes this instance to [S].
  S serialize();

  /// Computes the signature of this instance.
  String computeSignature();

  /// Computes a generic signature for [SerializableData] and basic Dart types:
  /// primitive values (null, num, bool, double, String),
  /// lists and maps whose elements are any of these.
  static String computeGenericSignature<D>(D data) {
    if (data is SerializableData) {
      return data.computeSignature();
    } else if (data is int) {
      return 'int>$data';
    } else if (data is double) {
      return 'double>$data';
    } else if (data is num) {
      return 'num>$data';
    } else if (data is String) {
      var h = data.hashCode;
      return 'str>$h';
    } else if (data is bool) {
      return 'bool>$data';
    } else if (data is Float32x4) {
      return 'f32x4>$data';
    } else if (data is Int32x4) {
      return 'i32x4>$data';
    } else if (data is Float64x2) {
      return 'i64x2>$data';
    } else if (data is List) {
      var h = data.computeHashcode();
      return 'list>$h';
    } else if (data is Map) {
      var h = data.computeHashcode();
      return 'map>$h';
    } else if (data is Set) {
      var h = data.computeHashcode();
      return 'map>$h';
    } else if (data is Iterable) {
      var h = data.computeHashcode();
      return 'map>$h';
    } else {
      var h = data.hashCode;
      return 'obj:$h';
    }
  }

  /// Creates generic copy for [SerializableData] and basic Dart types:
  /// primitive values (null, num, bool, double, String),
  /// lists and maps whose elements are any of these.
  static D copyGeneric<D>(D data) {
    if (data is SerializableData) {
      return data.copy() as D;
    } else if (data is int) {
      return data;
    } else if (data is double) {
      return data;
    } else if (data is num) {
      return data;
    } else if (data is String) {
      return data;
    } else if (data is bool) {
      return data;
    } else if (data is Float32x4) {
      return data;
    } else if (data is Int32x4) {
      return data;
    } else if (data is Float64x2) {
      return data;
    } else if (data is MapEntry) {
      return MapEntry(copyGeneric(data.key), copyGeneric(data.key)) as D;
    } else if (data is Uint8List) {
      return Uint8List.fromList(data) as D;
    } else if (data is Uint16List) {
      return Uint16List.fromList(data) as D;
    } else if (data is Uint32List) {
      return Uint32List.fromList(data) as D;
    } else if (data is Int8List) {
      return Int8List.fromList(data) as D;
    } else if (data is Int16List) {
      return Int8List.fromList(data) as D;
    } else if (data is Int32List) {
      return Int32List.fromList(data) as D;
    } else if (data is Float32List) {
      return Float32List.fromList(data) as D;
    } else if (data is Float64List) {
      return Float32List.fromList(data) as D;
    } else if (data is Float32x4List) {
      return Float32x4List.fromList(data) as D;
    } else if (data is Int32x4List) {
      return Int32x4List.fromList(data) as D;
    } else if (data is Float64x2List) {
      return Float64x2List.fromList(data) as D;
    } else if (data is List) {
      return data.deepCopy() as D;
    } else if (data is Map) {
      return data.deepCopy() as D;
    } else if (data is Set) {
      return data.deepCopy() as D;
    } else if (data is Iterable) {
      return data.deepCopy() as D;
    } else {
      throw StateError(
          "Can't perform a generic copy on type: ${data.runtimeType}");
    }
  }

  /// Generic serialization of [SerializableData] and basic Dart types:
  /// primitive values (null, num, bool, double, String),
  /// lists and maps whose elements are any of these.
  static S serializeGeneric<S>(dynamic data) {
    if (data is SerializableData) {
      return data.serialize() as S;
    } else if (data is int) {
      return data as S;
    } else if (data is double) {
      return data as S;
    } else if (data is num) {
      return data as S;
    } else if (data is String) {
      return data as S;
    } else if (data is bool) {
      return data as S;
    } else if (data is Float32x4) {
      return data as S;
    } else if (data is Int32x4) {
      return data as S;
    } else if (data is Float64x2) {
      return data as S;
    } else if (data is MapEntry) {
      return MapEntry(serializeGeneric(data.key), serializeGeneric(data.key))
          as S;
    } else if (data is Uint8List) {
      return data.serialize() as S;
    } else if (data is Uint16List) {
      return data.serialize() as S;
    } else if (data is Uint32List) {
      return data.serialize() as S;
    } else if (data is Int8List) {
      return data.serialize() as S;
    } else if (data is Int16List) {
      return data.serialize() as S;
    } else if (data is Int32List) {
      return data.serialize() as S;
    } else if (data is Float32List) {
      return data.serialize() as S;
    } else if (data is Float64List) {
      return data.serialize() as S;
    } else if (data is Float32x4List) {
      return data.serialize() as S;
    } else if (data is Int32x4List) {
      return data.serialize() as S;
    } else if (data is Float64x2List) {
      return data.serialize() as S;
    } else if (data is List) {
      return data.serialize() as S;
    } else if (data is Map) {
      return data.serialize() as S;
    } else if (data is Set) {
      return data.serialize() as S;
    } else if (data is Iterable) {
      return data.serialize() as S;
    } else {
      throw StateError(
          "Can't perform a generic serialization on type: ${data.runtimeType}");
    }
  }

  static Type _toType<T>() => T;

  static final _typeListInt = _toType<List<int>>();
  static final _typeListDouble = _toType<List<double>>();
  static final _typeListNum = _toType<List<num>>();
  static final _typeListBool = _toType<List<bool>>();
  static final _typeListString = _toType<List<String>>();

  static final _typeSetInt = _toType<Set<int>>();
  static final _typeSetDouble = _toType<Set<double>>();
  static final _typeSetNum = _toType<Set<num>>();
  static final _typeSetBool = _toType<Set<bool>>();
  static final _typeSetString = _toType<Set<String>>();

  static final _typeIterableInt = _toType<Iterable<int>>();
  static final _typeIterableDouble = _toType<Iterable<double>>();
  static final _typeIterableNum = _toType<Iterable<num>>();
  static final _typeIterableBool = _toType<Iterable<bool>>();
  static final _typeIterableString = _toType<Iterable<String>>();

  static bool _isBasicCollectionType(Type t) {
    return (t == _typeListInt ||
        t == _typeListDouble ||
        t == _typeListNum ||
        t == _typeListBool ||
        t == _typeListString ||
        t == _typeSetInt ||
        t == _typeSetDouble ||
        t == _typeSetNum ||
        t == _typeSetBool ||
        t == _typeSetString ||
        t == _typeIterableInt ||
        t == _typeIterableDouble ||
        t == _typeIterableNum ||
        t == _typeIterableBool ||
        t == _typeIterableString);
  }

  static D deserializeGeneric<D>(dynamic serial) {
    if (D == int) {
      return serial as D;
    } else if (D == double) {
      return serial as D;
    } else if (D == num) {
      return serial as D;
    } else if (D == String) {
      return serial as D;
    } else if (D == bool) {
      return serial as D;
    } else if (D == Float32x4) {
      return serial as D;
    } else if (D == Int32x4) {
      return serial as D;
    } else if (D == Float64x2) {
      return serial as D;
    } else if (D == MapEntry) {
      var e = serial as MapEntry;
      return MapEntry(deserializeGeneric(e.key), deserializeGeneric(e.key))
          as D;
    } else if (D == Uint8List) {
      return Uint8List.fromList(serial as List<int>) as D;
    } else if (D == Uint16List) {
      return Uint16List.fromList(serial as List<int>) as D;
    } else if (D == Uint32List) {
      return Uint32List.fromList(serial as List<int>) as D;
    } else if (D == Int8List) {
      return Int8List.fromList(serial as List<int>) as D;
    } else if (D == Int16List) {
      return Int16List.fromList(serial as List<int>) as D;
    } else if (D == Int32List) {
      return Int32List.fromList(serial as List<int>) as D;
    } else if (D == Float32List) {
      return Float32List.fromList(serial as List<double>) as D;
    } else if (D == Float64List) {
      return Float64List.fromList(serial as List<double>) as D;
    } else if (D == Float32x4List) {
      var e = serial as List<double>;
      return e.toFloat32x4List() as D;
    } else if (D == Int32x4List) {
      var e = serial as List<int>;
      return e.toInt32x4List() as D;
    } else if (D == Float64x2List) {
      var e = serial as List<double>;
      return e.toFloat64x2List() as D;
    } else if (_isBasicCollectionType(D)) {
      return serial as D;
    } else if (D == List) {
      var e = serial as List;
      return e.map(deserializeGeneric).toList() as D;
    } else if (D == Map) {
      var e = serial as Map;
      return e as D;
    } else if (D == Set) {
      var e = serial as Set;
      return e.map(deserializeGeneric).toSet() as D;
    } else if (D == Iterable) {
      var e = serial as Iterable;
      return e.map(deserializeGeneric).toList() as D;
    } else {
      throw StateError("Can't perform a generic deserialization on type: $D");
    }
  }
}

/// Class for shared data between [AsyncTask] instances.
///
/// This instance will be transferred to the executor thread/isolate only
/// once, and not per task, avoiding the [Isolate] messaging bottle neck.
///
/// - The internal [data] should NOT be modified after instantiated.
/// - [S] should be a simple type that can be sent through threads/isolates or
///   serialized as JSON.
/// - The default implementation supports Dart basic types:
///   primitive values (null, num, bool, double, String),
///   lists and maps whose elements are any of these.
class SharedData<D, S> {
  /// The actual data.
  final D data;

  SharedData(this.data);

  /// Creates a copy of this instance.
  SharedData<D, S> copy() {
    var data = this.data;
    if (data is SerializableData) {
      return SharedData<D, S>(data.copy() as D);
    } else {
      throw StateError("Can't perform a copy");
    }
  }

  /// Deserialize a [SharedData] from [serial].
  SharedData<D, S> deserialize(S serial) {
    return SharedData(serial as D);
  }

  /// Serializes this instance to [S].
  S serialize() => SerializableData.serializeGeneric(data);

  S? _serial;

  /// Serializes this instances to [S] using a cache for recurrent calls.
  S serializeCached() {
    _serial ??= serialize();
    return _serial!;
  }

  String? _signature;

  /// The signature of [data], computed only once.
  ///
  /// The signature is used to identify this shared object between threads/isolates.
  String get signature => _signature ??= computeSignature();

  /// The computation of [signature].
  String computeSignature() => SerializableData.computeGenericSignature(data);

  /// Dispose any cached computation, like [signature] and [serializeCached].
  void reset() {
    _signature = null;
    _serial = null;
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

    if (_closed) {
      throw AsyncExecutorClosedError('Not ready: executor closed.');
    } else if (_closing != null) {
      logger.logWarn('Executing task while closing executor: $task');
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

  bool _closed = false;

  /// Returns `true` if this executor is already closed.
  bool get isClosed => _closed;

  Completer<bool>? _closing;

  /// Returns `true` if this executor is closing.
  bool get isClosing => _closing != null;

  /// Returns `true` if this executor is ready to receive tasks to execute.
  bool get isReady => !isClosed && !isClosing;

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

  /// Closes this instance.
  Future<bool> close() async {
    return true;
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
