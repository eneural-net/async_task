@Timeout(Duration(seconds: 120))
library;

import 'package:async_task/async_task.dart';
import 'package:async_task/async_task_extension.dart';
import 'package:collection/collection.dart';
import 'package:test/test.dart';

/// Integration tests running real executors. With `parallelism >= 1` the
/// executor is backed by `Isolate`s on the VM and falls back to the
/// single-thread (web-like) executor on other platforms.
void main() {
  group('AsyncTask (without executor)', () {
    test('execute: sync result', () {
      var task = _SumTask(2, 3);

      expect(task.isIdle, isTrue);
      expect(task.status, equals(AsyncTaskStatus.idle));
      expect('$task', equals('_SumTask([2, 3])[idle]'));
      expect(task.executionTime, isNull);

      var r = task.execute();
      expect(r, equals(5));

      expect(task.isFinished, isTrue);
      expect(task.isNotFinished, isFalse);
      expect(task.isSuccessful, isTrue);
      expect(task.hasError, isFalse);
      expect(task.wasSubmitted, isTrue);
      expect(task.result, equals(5));
      expect(task.error, isNull);
      expect(task.status, equals(AsyncTaskStatus.successful));
      expect(task.executionTime, isNotNull);
      expect('$task', startsWith('_SumTask([2, 3])[successful]{ result: 5'));
      expect(task.zone, isNotNull);
      expect(task.executorThread, isNull);
      expect(task.isInExecutionContext, isFalse);
    });

    test('execute: async result', () async {
      var task = _SumTask(2, 3, async: true);
      var r = await task.execute();
      expect(r, equals(5));
      expect(await task.waitResult(), equals(5));
      expect(task.isSuccessful, isTrue);
    });

    test('execute: sync error', () async {
      var task = _FailTask(7);

      expect(() => task.execute(), throwsA(isA<_TaskFailure>()));

      expect(task.hasError, isTrue);
      expect(task.isSuccessful, isFalse);
      expect(task.status, equals(AsyncTaskStatus.error));
      expect(
        task.error,
        isA<AsyncExecutorError>().having(
          (e) => e.cause,
          'cause',
          isA<_TaskFailure>().having((e) => e.code, 'code', 7),
        ),
      );
      expect('$task', contains('[error]'));

      await expectLater(task.waitResult(), throwsA(isA<AsyncExecutorError>()));
    });

    test('execute: async error', () async {
      var task = _FailTask(8, async: true);
      await expectLater(task.execute(), throwsA(isA<_TaskFailure>()));
      expect(task.hasError, isTrue);
      await expectLater(task.waitResult(), throwsA(isA<AsyncExecutorError>()));
    });

    test('status executing / toString trimming', () {
      var task = _SumTask(123456789, 987654321);
      task.submitTime = DateTime.now();
      expect(task.status, equals(AsyncTaskStatus.executing));
      expect('$task', startsWith('_SumTask([123456789...)[executing]'));
    });

    test('reset / copy', () {
      var task = _SumTask(1, 2);
      task.execute();
      expect(task.isFinished, isTrue);

      task.reset();
      expect(task.isFinished, isFalse);
      expect(task.isIdle, isTrue);
      expect(task.result, isNull);
      expect(task.initTime, isNull);
      expect(task.endTime, isNull);

      var copy = task.copy();
      expect(copy, isA<_SumTask>());
      expect(identical(copy, task), isFalse);
      expect(copy.parameters(), equals([1, 2]));
      expect(copy.isIdle, isTrue);
    });

    test('addOnFinishAsyncTask', () {
      var task = _SumTask(1, 2);

      var calls = <Object?>[];
      task.addOnFinishAsyncTask((t, result, error, s) => calls.add(result));
      // A failing trigger shouldn't affect other triggers or the result:
      task.addOnFinishAsyncTask((t, result, error, s) => throw StateError('x'));
      task.addOnFinishAsyncTask((t, result, error, s) => calls.add('2nd'));

      expect(task.execute(), equals(3));
      expect(calls, equals([3, '2nd']));

      expect(
        () => task.addOnFinishAsyncTask((t, r, e, s) {}),
        throwsStateError,
      );
    });

    test('channel without executor', () async {
      var task = _SumTask(1, 2);
      expect(task.channelResolved(), isNull);

      var channelFuture = task.channel();
      expect(channelFuture, isA<Future>());

      expect(task.resolveChannel((t, c) {}), isNull);
      expect(await channelFuture, isNull);
      expect(task.channel(), isNull);
    });
  });

  group('AsyncExecutorError', () {
    test('stackTrace', () {
      var s = StackTrace.current;
      expect(AsyncExecutorError('m', 'c', s).stackTrace, same(s));
      expect(
        AsyncExecutorError('m', 'c', 'line1\nline2').stackTrace,
        isA<StackTrace>(),
      );
      expect(
        '${AsyncExecutorError('m', 'c', ['line1', 'line2']).stackTrace}',
        equals('line1\nline2'),
      );
      expect(AsyncExecutorError('m').stackTrace, isNull);
    });

    test('copyWith / toString', () {
      var e = AsyncExecutorError('m', 'c');
      var e2 = e.copyWith(message: 'm2');
      expect(e2.message, equals('m2'));
      expect(e2.cause, equals('c'));

      var e3 = e.copyWith(cause: 'c3', stackTrace: 'st');
      expect(e3.message, equals('m'));
      expect(e3.cause, equals('c3'));
      expect('${e3.stackTrace}', equals('st'));

      expect(e.toString(full: false), equals('AsyncExecutorError: m'));
      expect(e.toString(), contains('-- Cause: c'));
    });

    test('AsyncExecutorClosedError', () {
      expect(AsyncExecutorClosedError('x'), isA<AsyncExecutorError>());
    });
  });

  group('AsyncTaskLoggerCaller', () {
    test('enabled / enabledExecution', () {
      var logs = <String>[];
      var logger = AsyncTaskLoggerCaller(
        (type, message, [error, stackTrace]) =>
            logs.add('$type:$message:${error ?? ''}'),
      );

      // Disabled by default:
      logger.logInfo('a');
      logger.logExecution('x', 1, 2);
      expect(logs, isEmpty);
      expect(logger.enabled, isFalse);
      expect(logger.enabledExecution, isFalse);

      logger.enabled = true;
      logger.logInfo('i');
      logger.logWarn('w');
      logger.logError('err', null);
      logger.logExecution('x', 1, 2); // execution logging still disabled.

      expect(logs, equals(['INFO:i:', 'WARN:w:', 'ERROR:null:err']));
      logs.clear();

      logger.enabledExecution = true;
      expect(logger.enabledExecution, isTrue);
      logger.logExecution('x', 1, 2);
      expect(logs, equals(['EXEC:x > 1 > 2:']));
      logs.clear();

      logger.enabledExecution = false;
      logger.logExecution('x', 1, 2);
      expect(logs, isEmpty);

      // Re-enabling `enabled` restores the execution logger state:
      logger.enabledExecution = true;
      logger.enabled = false;
      logger.logExecution('x', 1, 2);
      logger.logInfo('i');
      expect(logs, isEmpty);

      logger.enabled = true;
      logger.logExecution('y', 1, 2);
      expect(logs, equals(['EXEC:y > 1 > 2:']));
    });

    test('defaultAsyncTaskLogger', () {
      var printed = <String>[];
      runZoned(
        () {
          defaultAsyncTaskLogger('INFO', 'msg');
          defaultAsyncTaskLogger('ERROR', 'msg', 'err', 'stack');
          defaultAsyncTaskLogger('ERROR', null, 'err2', 'stack2');
          defaultAsyncTaskLogger('ERROR', null, 'err3');
        },
        zoneSpecification: ZoneSpecification(
          print: (self, parent, zone, line) => printed.add(line),
        ),
      );

      expect(
        printed,
        equals([
          '[INFO] msg',
          '[ERROR] msg',
          'err',
          'stack',
          '[ERROR] err2',
          'stack2',
          '[ERROR] err3',
        ]),
      );
    });
  });

  group('AsyncExecutor.parameterParallelism', () {
    test('values', () {
      var max = AsyncExecutor.maximumParallelism;
      expect(max, greaterThanOrEqualTo(1));

      expect(AsyncExecutor.parameterParallelism(value: 0), equals(0));
      expect(AsyncExecutor.parameterParallelism(value: -1), equals(0));
      expect(AsyncExecutor.parameterParallelism(value: max + 10), equals(max));
      expect(AsyncExecutor.parameterParallelism(), equals(2.clamp(0, max)));

      expect(
        AsyncExecutor.parameterParallelism(byPercentage: 1.0),
        equals(max),
      );
      expect(
        AsyncExecutor.parameterParallelism(byPercentage: 100),
        equals(max),
      );
      expect(
        AsyncExecutor.parameterParallelism(byPercentage: 50),
        equals(AsyncExecutor.parameterParallelism(byPercentage: 0.5)),
      );
      expect(AsyncExecutor.parameterParallelism(byPercentage: 0), equals(0));
    });

    test('parallelismPercentage constructor parameter', () async {
      var executor = AsyncExecutor(
        parallelismPercentage: 0,
        taskTypeRegister: _taskTypeRegister,
      );
      expect(executor.parallelism, equals(0));
      await executor.close();
    });
  });

  for (var parallelism in [0, 2]) {
    for (var sequential in [false, true]) {
      _executorTests(parallelism: parallelism, sequential: sequential);
    }
  }
}

void _executorTests({required int parallelism, required bool sequential}) {
  AsyncExecutor newExecutor({String name = ''}) => AsyncExecutor(
    name: name,
    sequential: sequential,
    parallelism: parallelism,
    taskTypeRegister: _taskTypeRegister,
  );

  group(
    'AsyncExecutor(parallelism: $parallelism, sequential: $sequential)',
    () {
      late AsyncExecutor executor;

      setUp(() {
        executor = newExecutor(name: 'test');
      });

      tearDown(() async {
        await executor.close();
        expect(executor.isClosed, isTrue);
      });

      test('lifecycle and status', () async {
        expect(executor.name, equals('test'));
        expect(executor.sequential, equals(sequential));
        expect(executor.isStarted, isFalse);
        expect(executor.isReady, isFalse);
        expect(executor.status, equals(AsyncExecutorStatus.idle));

        // Concurrent start calls:
        var start1 = executor.start();
        var start2 = executor.start();
        expect(await start1, isTrue);
        expect(await start2, isTrue);
        expect(await executor.start(), isTrue);

        expect(executor.isStarted, isTrue);
        expect(executor.isReady, isTrue);
        expect(executor.status, equals(AsyncExecutorStatus.ready));
        expect('$executor', contains('sequential: $sequential'));

        var close1 = executor.close();
        expect(executor.isClosing, isTrue);
        expect(executor.isReady, isFalse);
        expect(executor.status, equals(AsyncExecutorStatus.closing));

        // Concurrent close calls:
        var close2 = executor.close();
        expect(await close1, isTrue);
        expect(await close2, isTrue);

        expect(executor.isClosed, isTrue);
        expect(executor.status, equals(AsyncExecutorStatus.closed));

        // Already closed:
        expect(await executor.close(), isTrue);

        expect(
          () => executor.execute(_SumTask(1, 1)),
          throwsA(isA<AsyncExecutorClosedError>()),
        );
      });

      test('execute results (sync and async tasks)', () async {
        var tasks = [
          for (var i = 0; i < 10; ++i) _SumTask(i, 100, async: i.isEven),
        ];

        var results = await executor.executeAllAndWaitResults(tasks);
        expect(results, equals([for (var i = 0; i < 10; ++i) i + 100]));

        expect(tasks.allTasksFinished, isTrue);
        expect(tasks.allTasksSuccessful, isTrue);
        expect(tasks.tasksResults, equals(results));
        expect(
          tasks.tasksWithStatus(AsyncTaskStatus.successful).length,
          equals(10),
        );
        for (var t in tasks) {
          expect(t.executorThread, isNotNull);
          expect(t.executionTime, isNotNull);
          expect(t.isInExecutionContext, isFalse);
        }

        var info = executor.info;
        expect(info.sequential, equals(sequential));
        expect(info.workers, greaterThanOrEqualTo(1));
        expect(info.workers, lessThanOrEqualTo(executor.maximumWorkers));
        expect(info.threads.map((t) => t.dispatchedTasks).sum, equals(10));
        expect(info.threads.map((t) => t.executedTasks).sum, equals(10));
        for (var t in info.threads) {
          expect(t.executingTasks, equals(0));
          expect('$t', contains('executingTasks: 0'));
        }
        expect('$info', contains('workers: ${info.workers}'));
      });

      test('execute already submitted task', () async {
        var task = _SumTask(4, 5);
        var f1 = executor.execute(task);
        var f2 = executor.execute(task);
        expect(await f1, equals(9));
        expect(await f2, equals(9));
        expect(executor.info.threads.map((t) => t.dispatchedTasks).sum, 1);
      });

      test('executeAll with a non List Iterable', () async {
        var tasks = {_SumTask(1, 1), _SumTask(2, 2)};
        var results = await Future.wait(executor.executeAll(tasks));
        expect(results, equals([2, 4]));
      });

      test('AsyncTaskExtension helpers', () async {
        var tasks = [_SumTask(1, 1), _SumTask(2, 2), _SumTask(3, 3)];

        expect(tasks.tasksWithStatus(AsyncTaskStatus.idle).length, equals(3));
        expect(tasks.tasksResults, equals([null, null, null]));

        // Only the first task is submitted:
        var f0 = executor.execute(tasks[0]);
        expect(await tasks.waitResults(onlySubmitted: true), equals([2]));
        expect(await f0, equals(2));

        var waited = await tasks.waitTasks(onlySubmitted: true);
        expect(waited, equals([tasks[0]]));

        var futures = tasks.executeTasks(executor);
        expect(futures.length, equals(3));
        expect(await tasks.waitResults(), equals([2, 4, 6]));
        expect(await tasks.waitTasks(), equals(tasks));

        var tasks2 = [_SumTask(10, 1), _SumTask(20, 2)];
        expect(await tasks2.executeAndWaitResults(executor), equals([11, 22]));

        var tasks3 = [_SumTask(10, 3), _SumTask(20, 4)];
        var executed = await tasks3.executeAndWaitTasks(executor);
        expect(executed.map((t) => t.result), equals([13, 24]));
      });

      test('task errors propagate', () async {
        var okBefore = _SumTask(1, 1);
        var failing = _FailTask(42);
        var failingAsync = _FailTask(43, async: true);
        var okAfter = _SumTask(2, 2);

        var fOkBefore = executor.execute(okBefore);
        var fFailing = executor.execute(failing);
        var fFailingAsync = executor.execute(failingAsync);
        var fOkAfter = executor.execute(okAfter);

        Matcher failure(int code) => isA<AsyncExecutorError>()
            .having(
              (e) => e.message,
              'message',
              startsWith('AsyncTask execution error'),
            )
            .having(
              (e) => e.cause,
              'cause',
              isA<_TaskFailure>().having((e) => e.code, 'code', code),
            );

        await expectLater(fFailing, throwsA(failure(42)));
        await expectLater(fFailingAsync, throwsA(failure(43)));

        // Errors don't affect other tasks (also in sequential mode):
        expect(await fOkBefore, equals(2));
        expect(await fOkAfter, equals(4));

        var all = [okBefore, failing, failingAsync, okAfter];
        expect(all.allTasksFinished, isTrue);
        expect(all.hasTaskWithError, isTrue);
        expect(all.allTasksSuccessful, isFalse);
        expect(all.errorTasks, equals([failing, failingAsync]));
        expect(all.successfulTasks, equals([okBefore, okAfter]));
        expect(failing.status, equals(AsyncTaskStatus.error));
        expect(failing.error, failure(42));

        // The executor is still usable after task errors:
        expect(await executor.execute(_SumTask(5, 5)), equals(10));
      });

      test('shared data', () async {
        var base = SharedData<List<int>, List<int>>([10, 20, 30]);

        var sharedDataInfo = AsyncExecutorSharedDataInfo();
        var tasks = [for (var i = 0; i < 6; ++i) _SharedSumTask(i, base)];

        var results = await executor.executeAllAndWaitResults(
          tasks,
          sharedDataInfo: sharedDataInfo,
        );
        expect(results, equals([for (var i = 0; i < 6; ++i) 60 + i]));

        if (executor.platform.isNative) {
          // Sent once per thread, identified by the same signature:
          expect(
            sharedDataInfo.sentSharedDataSignatures,
            equals({base.signature}),
          );
        } else {
          expect(sharedDataInfo.sentSharedDataSignatures, isEmpty);
        }

        expect(await executor.disposeSharedDataInfo(sharedDataInfo), isTrue);
        expect(
          sharedDataInfo.disposedSharedDataSignatures,
          equals(sharedDataInfo.sentSharedDataSignatures),
        );

        // After the dispose the `SharedData` is sent again when needed:
        var sharedDataInfo2 = AsyncExecutorSharedDataInfo();
        var results2 = await executor.executeAllAndWaitResults(
          [_SharedSumTask(100, base), _SharedSumTask(200, base)],
          sharedDataInfo: sharedDataInfo2,
          disposeSentSharedData: true,
        );
        expect(results2, equals([160, 260]));
        expect(
          sharedDataInfo2.disposedSharedDataSignatures,
          equals(sharedDataInfo2.sentSharedDataSignatures),
        );

        // Without an explicit `sharedDataInfo`:
        var results3 = await executor.executeAllAndWaitResults([
          _SharedSumTask(1000, base),
        ], disposeSentSharedData: true);
        expect(results3, equals([1060]));

        // Disposing with `async: true` (fire and forget):
        var sharedDataInfo4 = AsyncExecutorSharedDataInfo();
        expect(
          await executor.execute(
            _SharedSumTask(1, base),
            sharedDataInfo: sharedDataInfo4,
          ),
          equals(61),
        );
        expect(
          await executor.disposeSharedData(
            sharedDataInfo4.sentSharedDataSignatures,
            async: true,
          ),
          isTrue,
        );
        expect(
          await executor.disposeSharedDataInfo(sharedDataInfo4, async: true),
          isTrue,
        );
        expect(await executor.disposeSharedData({}), isTrue);

        // Still works after the async dispose:
        expect(await executor.execute(_SharedSumTask(2, base)), equals(62));
      });

      test('channel with messageHandler and message queue', () async {
        var task = _ChannelTask(7);
        var future = executor.execute(task);

        var channel = (await task.channel())!;
        expect(channel.isInitialized, isTrue);
        expect(channel.task, same(task));
        expect(channel.isInExecutionContext, isFalse);
        expect('$channel', contains('initialized: true'));

        var first = await channel.waitMessage<int>();
        expect(first, equals(7));

        channel.send(first * 10);

        var second = await channel.waitMessage<String>();
        expect(second, equals('done:70'));
        expect(channel.messageQueueLength, equals(0));
        expect(channel.messageQueueIsEmpty, isTrue);
        expect(channel.messageQueueIsNotEmpty, isFalse);
        expect(channel.readMessage(), isNull);

        channel.send('ok');

        expect(await future, equals(72));
        expect(channel.isClosed, isTrue);

        // The handler of the caller side received the messages from the task:
        expect(
          task.handled,
          containsAllInOrder([(7, true), ('done:70', true)]),
        );
      });

      test('close waits for executing tasks', () async {
        var tasks = [for (var i = 0; i < 4; ++i) _SumTask(i, 1, async: true)];
        var futures = executor.executeAll(tasks);

        await executor.close();
        expect(executor.isClosed, isTrue);

        expect(await Future.wait(futures), equals([1, 2, 3, 4]));
        expect(tasks.allTasksSuccessful, isTrue);
      });
    },
  );
}

List<AsyncTask> _taskTypeRegister() => <AsyncTask>[
  _SumTask(0, 0),
  _FailTask(0),
  _SharedSumTask(0, SharedData<List<int>, List<int>>([])),
  _ChannelTask(0),
];

class _SumTask extends AsyncTask<List<int>, int> {
  final int a;
  final int b;
  final bool async;

  _SumTask(this.a, this.b, {this.async = false});

  @override
  AsyncTask<List<int>, int> instantiate(
    List<int> parameters, [
    Map<String, SharedData>? sharedData,
  ]) => _SumTask(parameters[0], parameters[1], async: parameters.length > 2);

  @override
  List<int> parameters() => [a, b, if (async) 1];

  @override
  FutureOr<int> run() {
    if (async) {
      return Future.microtask(() => a + b);
    }
    return a + b;
  }
}

class _TaskFailure {
  final int code;

  _TaskFailure(this.code);

  @override
  String toString() => '_TaskFailure($code)';
}

class _FailTask extends AsyncTask<List<int>, int> {
  final int code;
  final bool async;

  _FailTask(this.code, {this.async = false});

  @override
  AsyncTask<List<int>, int> instantiate(
    List<int> parameters, [
    Map<String, SharedData>? sharedData,
  ]) => _FailTask(parameters[0], async: parameters[1] == 1);

  @override
  List<int> parameters() => [code, async ? 1 : 0];

  @override
  FutureOr<int> run() {
    if (async) {
      return Future.microtask(() => throw _TaskFailure(code));
    }
    throw _TaskFailure(code);
  }
}

class _SharedSumTask extends AsyncTask<int, int> {
  final int n;
  final SharedData<List<int>, List<int>> base;

  _SharedSumTask(this.n, this.base);

  @override
  AsyncTask<int, int> instantiate(
    int parameters, [
    Map<String, SharedData>? sharedData,
  ]) => _SharedSumTask(
    parameters,
    sharedData!['base'] as SharedData<List<int>, List<int>>,
  );

  @override
  Map<String, SharedData>? sharedData() => {'base': base};

  @override
  SharedData? loadSharedData(String key, dynamic serial) {
    if (key != 'base') throw StateError('Unknown key: $key');
    return SharedData<List<int>, List<int>>(List<int>.from(serial as List));
  }

  @override
  int parameters() => n;

  @override
  FutureOr<int> run() => base.data.sum + n;
}

class _ChannelTask extends AsyncTask<int, int> {
  final int n;

  final List<(Object?, bool)> handled = [];

  _ChannelTask(this.n);

  @override
  AsyncTask<int, int> instantiate(
    int parameters, [
    Map<String, SharedData>? sharedData,
  ]) => _ChannelTask(parameters);

  @override
  int parameters() => n;

  @override
  AsyncTaskChannel? channelInstantiator() => AsyncTaskChannel(
    messageHandler: (message, fromExecutingContext) =>
        handled.add((message, fromExecutingContext)),
  );

  @override
  FutureOr<int> run() async {
    var channel = channelResolved()!;
    if (!channel.isInExecutionContext) {
      throw StateError('Expected to run in the execution context');
    }

    var reply = await channel.sendAndWaitResponse<int, int>(n);
    // Wait for an acknowledgement, so the caller reads this message before the
    // task finishes (and its channel is closed):
    var ack = await channel.sendAndWaitResponse<String, String>('done:$reply');
    return reply + ack.length;
  }
}
