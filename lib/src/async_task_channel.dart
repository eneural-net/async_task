import 'dart:async';

import 'package:async_task/src/async_task_base.dart';
import 'package:collection/collection.dart';

/// An [AsyncTaskChannel] message handler.
///
/// - [message] the message.
/// - [fromExecutingContext] if `true`, indicated that the message comes from
///   the executing context (from [AsyncTask.run]). If `false` indicates that
///   the message comes from outside the task.
typedef AsyncTaskChannelMessageHandler = void Function(
    dynamic message, bool fromExecutingContext);

/// A message channel for a running [AsyncTask]
class AsyncTaskChannel {
  static int _idCount = 0;

  final int id = ++_idCount;

  /// Optional handler that will receive all messages.
  final AsyncTaskChannelMessageHandler? _messageHandler;

  /// Constructs a task channel.
  ///
  /// - [messageHandler]: optional handler that will receive all messages.
  AsyncTaskChannel({AsyncTaskChannelMessageHandler? messageHandler})
      : _messageHandler = messageHandler;

  late final AsyncTask _task;

  /// Returns this channel [task].
  AsyncTask get task => _task;

  late final AsyncTaskChannelPort _port;

  AsyncTaskChannelPort get port => _port;

  bool _initialized = false;

  /// Returns `true` if this channel is initialized.
  bool get isInitialized => _initialized;

  /// Initialize this channel. Called by [AsyncExecutor].
  void initialize(AsyncTask task, AsyncTaskChannelPort port) {
    if (_initialized) return;
    _task = task;
    _port = port;
    _initialized = true;
  }

  /// If `true`, indicated that channel calls are coming from [AsyncTask.run].
  /// If `false` indicates that calls are from outside the [AsyncTask].
  bool get isInExecutionContext => _port.isInExecutionContext(_task);

  /// Sends [message].
  ///
  /// - If [isInExecutionContext] it will send the [message] to the executing
  /// task ([AsyncTask.run]).
  void send<M>(M message) => _port.send<M>(message, isInExecutionContext);

  /// Calls [send] and [waitMessage].
  Future<R> sendAndWaitResponse<M, R>(M message) {
    send<M>(message);
    return waitMessage<R>();
  }

  /// Waits for a message to arrive and return it.
  Future<M> waitMessage<M>() => _port.read<M>(isInExecutionContext);

  /// Reads a message if available in queue or return `null` (non-blocking).
  M? readMessage<M>() => _port.readSync<M>(isInExecutionContext);

  /// Returns the current message queue length.
  int get messageQueueLength => _port.messageQueueLength(isInExecutionContext);

  /// Returns `true` if the message queue is empty.
  bool get messageQueueIsEmpty => messageQueueLength == 0;

  /// Returns `true` if the message queue is NOT empty.
  bool get messageQueueIsNotEmpty => !messageQueueIsEmpty;

  void _notifyMessage(message, bool fromExecutingContext) {
    var messageHandler = _messageHandler;
    if (messageHandler != null) {
      try {
        messageHandler(message, fromExecutingContext);
      } catch (e, s) {
        print(e);
        print(s);
      }
    }
  }

  /// Returns `true` if this channel is closed. An [AsyncTaskChannel] is closed
  /// when a task is finished.
  bool get isClosed => _initialized && _port.isClosed;

  /// Closes this channel.
  ///
  /// Will be called automatically when this channel [task] is closed.
  void close() {
    _port.close();
  }

  @override
  String toString() {
    return 'AsyncTaskChannel{id: $id, initialized: $isInitialized}';
  }
}

/// Base class for channels ports.
abstract class AsyncTaskChannelPort {
  static int _idCount = 0;

  final int id = ++_idCount;

  final AsyncTaskChannel _channel;

  AsyncTaskChannelPort(this._channel);

  bool _closed = false;

  bool get isClosed => _closed;

  /// Closes this port. If it's already closed, a call will have no effect.
  void close() {
    _closed = true;
  }

  /// if `true`, indicates that the current context is inside [AsyncTask.run].
  bool isInExecutionContext(AsyncTask task) => task.isInExecutionContext;

  /// Sends [message] to the other context.
  ///
  /// - [inExecutingContext] the current context. See [isInExecutionContext].
  void send<M>(M message, bool inExecutingContext);

  /// When a message is received by this port implementation.
  void onReceiveMessage(dynamic message, bool fromExecutingContext) {
    if (isClosed) throw StateError('Close port: $this');

    _channel._notifyMessage(message, fromExecutingContext);

    QueueList<dynamic> messageQueue;
    QueueList<Completer> readQueue;

    if (fromExecutingContext) {
      messageQueue = _messageQueue;
      readQueue = _readQueue;
    } else {
      messageQueue = _messageQueueExecutingContext;
      readQueue = _readQueueExecutingContext;
    }

    if (readQueue.isNotEmpty) {
      var reader = readQueue.removeFirst();
      reader.complete(message);
    } else {
      messageQueue.addLast(message);
    }
  }

  final QueueList<dynamic> _messageQueue = QueueList<dynamic>(32);

  final QueueList<dynamic> _messageQueueExecutingContext =
      QueueList<dynamic>(32);

  final QueueList<Completer> _readQueue = QueueList<Completer>(32);

  final QueueList<Completer> _readQueueExecutingContext =
      QueueList<Completer>(32);

  /// Reads a message.
  ///
  /// - [inExecutingContext] the current context. See [isInExecutionContext].
  Future<T> read<T>(bool inExecutingContext) {
    QueueList<dynamic> messageQueue;
    QueueList<Completer> readQueue;

    if (inExecutingContext) {
      messageQueue = _messageQueueExecutingContext;
      readQueue = _readQueueExecutingContext;
    } else {
      messageQueue = _messageQueue;
      readQueue = _readQueue;
    }

    if (messageQueue.isNotEmpty) {
      var msg = messageQueue.removeFirst() as T;
      return Future.value(msg);
    } else {
      var completer = Completer<T>();
      readQueue.addLast(completer);
      return completer.future;
    }
  }

  /// Reads a message from queue if present or return `null`.
  T? readSync<T>(bool inExecutingContext) {
    QueueList<dynamic> messageQueue;

    if (inExecutingContext) {
      messageQueue = _messageQueueExecutingContext;
    } else {
      messageQueue = _messageQueue;
    }

    if (messageQueue.isNotEmpty) {
      var msg = messageQueue.removeFirst() as T;
      return msg;
    } else {
      return null;
    }
  }

  /// Returns the length of the message queue.
  int messageQueueLength(bool inExecutingContext) {
    QueueList<dynamic> messageQueue;

    if (inExecutingContext) {
      messageQueue = _messageQueueExecutingContext;
    } else {
      messageQueue = _messageQueue;
    }

    return messageQueue.length;
  }

  @override
  String toString() {
    return 'AsyncTaskChannelPort{ id: $id, executingContext: [m: ${_messageQueueExecutingContext.length}, r: ${_readQueueExecutingContext.length}], caller: [m: ${_messageQueue.length}, r: ${_readQueue.length}] }';
  }
}
