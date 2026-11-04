@Timeout(Duration(seconds: 120))
library;

import 'package:async_task/async_task.dart';
import 'package:async_task/async_task_extension.dart';
import 'package:test/test.dart';

/// Stress/integration tests: hundreds of tasks per executor configuration.
///
/// All the assertions are about results, ordering and counters (no wall-clock
/// thresholds), so they are deterministic on slow machines.
///
/// On the Web every configuration runs in the generic (single thread)
/// executor, as `AsyncExecutor.maximumParallelism` is `1`.
void main() {
  for (var parallelism in [0, 1, 4]) {
    for (var sequential in [false, true]) {
      group('AsyncExecutor(parallelism: $parallelism, sequential: $sequential)', () {
        late AsyncExecutor executor;

        setUp(() {
          _StressTask.resetCounters();
          executor = AsyncExecutor(
            sequential: sequential,
            parallelism: parallelism,
            taskTypeRegister: _taskTypeRegister,
          );
        });

        tearDown(() => executor.close());

        bool isParallel() => executor.info.parallel;

        test('hundreds of mixed sync/async/throwing tasks', () async {
          const total = 400;
          var tasks = [
            for (var i = 0; i < total; ++i) _StressTask(i, _mode(i)),
          ];

          var executions = executor.executeAll(tasks);
          expect(executions.length, equals(total));

          var outcomes = await _settle(executions);
          expect(outcomes.length, equals(total));

          for (var i = 0; i < total; ++i) {
            var task = tasks[i];
            var outcome = outcomes[i];
            expect(task.isFinished, isTrue, reason: 'task #$i');

            if (_throws(i)) {
              expect(task.hasError, isTrue, reason: 'task #$i');
              expect(task.status, equals(AsyncTaskStatus.error));
              expect(identical(outcome, task.error), isTrue);
              expect(
                outcome,
                isA<AsyncExecutorError>()
                    .having(
                      (e) => e.message,
                      'message',
                      startsWith('AsyncTask execution error'),
                    )
                    .having(
                      (e) => e.cause,
                      'cause',
                      isA<StateError>().having(
                        (e) => e.message,
                        'message',
                        equals('fail#$i'),
                      ),
                    ),
                reason: 'task #$i',
              );
            } else {
              expect(task.isSuccessful, isTrue, reason: 'task #$i');
              var result = outcome as List<int>;
              // Results are in the same order as the submitted tasks:
              expect(result[0], equals(i * 2), reason: 'task #$i');
              expect(identical(result, task.result), isTrue);
            }

            if (_throws(i) && isParallel()) {
              // Current behaviour: the `Isolate` error reply doesn't carry the
              // execution times (the generic executor sets them).
              expect(task.initTime, isNull, reason: 'task #$i');
              expect(task.endTime, isNull, reason: 'task #$i');
            } else {
              expect(task.initTime, isNotNull, reason: 'task #$i');
              expect(task.endTime, isNotNull, reason: 'task #$i');
              expect(task.endTime!.isBefore(task.initTime!), isFalse);
            }
          }

          var errors = tasks.where((t) => t.hasError).length;
          expect(
            errors,
            equals(List.generate(total, _throws).where((e) => e).length),
          );
          expect(tasks.successfulTasks.length, equals(total - errors));
          expect(tasks.errorTasks.length, equals(errors));
          expect(tasks.allTasksFinished, isTrue);

          var info = executor.info;
          var dispatched = _sum(info.threads.map((t) => t.dispatchedTasks));
          var executed = _sum(info.threads.map((t) => t.executedTasks));
          expect(dispatched, equals(total));
          expect(executed, equals(total));
          expect(info.threads.every((t) => t.executingTasks == 0), isTrue);
          expect(info.workers, lessThanOrEqualTo(executor.maximumWorkers));

          if (!isParallel()) {
            // Same isolate: the task bodies were executed here and in the
            // submission order (the executor dispatches them in order):
            expect(
              _StressTask.executionOrder,
              equals(List.generate(total, (i) => i)),
            );

            if (sequential) {
              // Never more than one task running at the same time:
              expect(_StressTask.maxRunning, equals(1));
            } else {
              // All the async bodies start before any of them can resume:
              expect(
                _StressTask.maxRunning,
                equals(List.generate(total, _isAsync).where((e) => e).length),
              );
            }
          } else {
            // Executed in other isolates:
            expect(_StressTask.executionOrder, isEmpty);

            if (sequential) {
              // Each isolate runs only one task at a time:
              for (var i = 0; i < total; ++i) {
                if (_throws(i)) continue;
                expect(tasks[i].result![1], equals(1), reason: 'task #$i');
              }
            }
          }
        });

        test('executing the same task again returns the same result', () async {
          var task = _StressTask(21, 1);
          var f1 = executor.execute(task);
          var f2 = executor.execute(task);

          var r1 = await f1;
          var r2 = await f2;
          expect(r1[0], equals(42));
          expect(identical(r1, r2), isTrue);

          // Already finished: not executed again.
          var r3 = await executor.execute(task);
          expect(identical(r1, r3), isTrue);

          var executed = _sum(
            executor.info.threads.map((t) => t.executedTasks),
          );
          expect(executed, equals(1));
        });

        test('executeAll with a non List Iterable keeps the order', () async {
          var tasks = Iterable.generate(50, (i) => _StressTask(i, i % 2));
          var results = await executor.executeAllAndWaitResults(tasks);
          expect(
            results.map((r) => r[0]),
            equals([for (var i = 0; i < 50; ++i) i * 2]),
          );
        });

        test('close while tasks are queued', () async {
          const total = 120;
          var tasks = [
            for (var i = 0; i < total; ++i) _StressTask(i, _mode(i)),
          ];

          // Not started yet: the tasks are queued until the executor starts.
          expect(executor.isStarted, isFalse);
          var executions = executor.executeAll(tasks);

          // Close without waiting for the results:
          var closing = executor.close();
          expect(executor.isClosing, isTrue);
          expect(executor.status, equals(AsyncExecutorStatus.closing));

          var outcomes = await _settle(executions);
          expect(await closing, isTrue);
          expect(executor.isClosed, isTrue);
          expect(executor.status, equals(AsyncExecutorStatus.closed));

          // No task was lost:
          for (var i = 0; i < total; ++i) {
            if (_throws(i)) {
              expect(
                outcomes[i],
                isA<AsyncExecutorError>(),
                reason: 'task #$i',
              );
            } else {
              expect(
                (outcomes[i] as List<int>)[0],
                equals(i * 2),
                reason: 'task #$i',
              );
            }
          }

          expect(
            () => executor.execute(_StressTask(0, 0)),
            throwsA(isA<AsyncExecutorClosedError>()),
          );

          // Closing again is a no-op:
          expect(await executor.close(), isTrue);
        });

        test('close while started tasks are running', () async {
          await executor.start();

          const total = 120;
          var tasks = [
            for (var i = 0; i < total; ++i) _StressTask(i, _mode(i)),
          ];
          var executions = executor.executeAll(tasks);

          var closing = executor.close();
          var outcomes = await _settle(executions);
          expect(await closing, isTrue);

          for (var i = 0; i < total; ++i) {
            if (_throws(i)) {
              expect(
                outcomes[i],
                isA<AsyncExecutorError>(),
                reason: 'task #$i',
              );
            } else {
              expect(
                (outcomes[i] as List<int>)[0],
                equals(i * 2),
                reason: 'task #$i',
              );
            }
          }

          if (isParallel()) {
            expect(executor.info.workers, equals(0));
          }
        });

        test('execute while closing', () async {
          await executor.start();

          var running = [for (var i = 0; i < 20; ++i) _StressTask(i, 1)];
          var executions = executor.executeAll(running);

          var closing = executor.close();
          expect(executor.isClosing, isTrue);

          // Still accepted (logs a warning) and executed:
          var lateExecution = executor.execute(_StressTask(100, 1));

          expect(
            (await Future.wait(executions)).map((r) => r[0]),
            equals([for (var i = 0; i < 20; ++i) i * 2]),
          );
          expect((await lateExecution)[0], equals(200));
          expect(await closing, isTrue);
          expect(executor.isClosed, isTrue);
        });

        test('channel message handler errors are caught', () async {
          var handled = <Object?>[];
          var tasks = [
            for (var i = 0; i < 20; ++i)
              _PingPongTask(
                i,
                3,
                messageHandler: (m, fromExecutingContext) {
                  handled.add(m);
                  throw StateError('handler error');
                },
              ),
          ];
          var executions = executor.executeAll(tasks);

          for (var task in tasks) {
            var channel = (await task.channel())!;
            for (var r = 0; r < 3; ++r) {
              var m = await channel.waitMessage<int>();
              channel.send(m + 1);
            }
            expect(
              await channel.sendAndWaitResponse<int, int>(-task.id),
              equals(task.id * 2),
            );
          }

          var results = await Future.wait(executions);
          for (var i = 0; i < tasks.length; ++i) {
            expect(
              results[i],
              equals(_sum([for (var r = 0; r < 3; ++r) i * 1000 + r + 1]) - i),
            );
          }

          // The handler only runs on this side for the messages received here
          // (in the same isolate it also sees the messages sent to the task):
          var fromTasks = [
            for (var i = 0; i < tasks.length; ++i)
              for (var r = 0; r < 3; ++r) i * 1000 + r,
          ];
          expect(handled.toSet().containsAll(fromTasks), isTrue);
        });

        test('shared data reused across many tasks', () async {
          var values = SharedData<List<int>, List<int>>([1, 2, 3, 4]);
          var offsets = [
            for (var i = 0; i < 3; ++i) SharedData<int, int>(i * 100),
          ];

          const total = 300;
          List<_SharedSumTask> newTasks() => [
            for (var i = 0; i < total; ++i)
              _SharedSumTask(i, values, offsets[i % offsets.length]),
          ];

          int expected(int i) => 10 + (i % offsets.length) * 100 + i;

          var sharedDataInfo = AsyncExecutorSharedDataInfo();
          var results = await executor.executeAllAndWaitResults(
            newTasks(),
            sharedDataInfo: sharedDataInfo,
          );
          expect(
            results,
            equals([for (var i = 0; i < total; ++i) expected(i)]),
          );

          var signatures = {
            values.signature,
            for (var o in offsets) o.signature,
          };
          expect(signatures.length, equals(4));

          if (executor.platform.isNative) {
            expect(sharedDataInfo.sentSharedDataSignatures, equals(signatures));
          } else {
            // No `SharedData` transfer in the same isolate:
            expect(sharedDataInfo.sentSharedDataSignatures, isEmpty);
          }

          // Dispose and reuse: the `SharedData` is sent again.
          await executor.disposeSharedDataInfo(sharedDataInfo);
          if (executor.platform.isNative) {
            expect(
              sharedDataInfo.disposedSharedDataSignatures,
              equals(signatures),
            );
          }

          var sharedDataInfo2 = AsyncExecutorSharedDataInfo();
          var results2 = await executor.executeAllAndWaitResults(
            newTasks(),
            sharedDataInfo: sharedDataInfo2,
            disposeSentSharedData: true,
          );
          expect(results2, equals(results));

          if (executor.platform.isNative) {
            expect(
              sharedDataInfo2.sentSharedDataSignatures,
              equals(signatures),
            );
            expect(
              sharedDataInfo2.disposedSharedDataSignatures,
              equals(signatures),
            );
          }

          // Asynchronous dispose (doesn't wait the isolates confirmation):
          expect(
            await executor.disposeSharedData(signatures, async: true),
            isTrue,
          );
          expect(
            await executor.disposeSharedDataInfo(sharedDataInfo2, async: true),
            isTrue,
          );
          // Nothing to dispose:
          expect(await executor.disposeSharedData(<String>{}), isTrue);
          expect(
            await executor.disposeSharedDataInfo(AsyncExecutorSharedDataInfo()),
            isTrue,
          );

          var results3 = await executor.executeAllAndWaitResults(newTasks());
          expect(results3, equals(results));
        });

        test('channels: bidirectional messages under load', () async {
          const totalTasks = 40;
          const rounds = 15;

          var tasks = [
            for (var i = 0; i < totalTasks; ++i) _PingPongTask(i, rounds),
          ];
          var executions = executor.executeAll(tasks);

          // Serve all the channels concurrently:
          var conversations = tasks.map((task) async {
            var channel = (await task.channel())!;
            expect(channel.isInExecutionContext, isFalse);

            var received = <int>[];
            for (var r = 0; r < rounds; ++r) {
              // Message from the task:
              var m = await channel.waitMessage<int>();
              received.add(m);
              // Reply to the task:
              channel.send(m + 1);
            }

            // A message started from this side, with a reply from the task:
            var ack = await channel.sendAndWaitResponse<int, int>(-task.id);
            expect(ack, equals(task.id * 2));

            return received;
          }).toList();

          var received = await Future.wait(conversations);
          var results = await Future.wait(executions);

          for (var i = 0; i < totalTasks; ++i) {
            expect(
              received[i],
              equals([for (var r = 0; r < rounds; ++r) i * 1000 + r]),
              reason: 'task #$i',
            );
            // The task sums the replies (`m + 1`) and the final message:
            var expected =
                _sum([for (var r = 0; r < rounds; ++r) i * 1000 + r + 1]) - i;
            expect(results[i], equals(expected), reason: 'task #$i');

            var channel = tasks[i].channelResolved()!;
            expect(channel.isClosed, isTrue);
          }
        });
      });
    }
  }
}

Future<List<Object?>> _settle<R>(List<Future<R>> futures) => Future.wait(
  futures.map((f) => f.then<Object?>((r) => r, onError: (Object e) => e)),
);

int _sum(Iterable<int> ns) => ns.fold(0, (a, b) => a + b);

bool _throws(int i) => i % 5 == 4;

bool _isAsync(int i) => _mode(i) == 1 || _mode(i) == 3;

/// - 0: sync result.
/// - 1: async result.
/// - 2: sync error.
/// - 3: async error.
int _mode(int i) => _throws(i) ? (i.isEven ? 2 : 3) : (i.isEven ? 0 : 1);

List<AsyncTask> _taskTypeRegister() => <AsyncTask>[
  _StressTask(0, 0),
  _SharedSumTask(0, SharedData([0]), SharedData(0)),
  _PingPongTask(0, 0),
];

class _StressTask extends AsyncTask<List<int>, List<int>> {
  /// Order of execution of [run] (only in the current isolate).
  static final List<int> executionOrder = <int>[];

  static int _running = 0;

  /// Maximum number of [run] executing at the same time
  /// (only in the current isolate).
  static int maxRunning = 0;

  static void resetCounters() {
    executionOrder.clear();
    _running = 0;
    maxRunning = 0;
  }

  final int id;
  final int mode;

  _StressTask(this.id, this.mode);

  @override
  AsyncTask<List<int>, List<int>> instantiate(
    List<int> parameters, [
    Map<String, SharedData>? sharedData,
  ]) => _StressTask(parameters[0], parameters[1]);

  @override
  List<int> parameters() => [id, mode];

  @override
  FutureOr<List<int>> run() {
    executionOrder.add(id);
    var running = ++_running;
    if (running > maxRunning) maxRunning = running;

    switch (mode) {
      case 0:
        --_running;
        return [id * 2, running];
      case 1:
        return Future.delayed(Duration.zero, () {
          --_running;
          return [id * 2, running];
        });
      case 2:
        --_running;
        throw StateError('fail#$id');
      default:
        return Future.delayed(Duration.zero, () {
          --_running;
          throw StateError('fail#$id');
        });
    }
  }
}

class _SharedSumTask extends AsyncTask<int, int> {
  final int id;
  final SharedData<List<int>, List<int>> values;
  final SharedData<int, int> offset;

  _SharedSumTask(this.id, this.values, this.offset);

  @override
  AsyncTask<int, int> instantiate(
    int parameters, [
    Map<String, SharedData>? sharedData,
  ]) => _SharedSumTask(
    parameters,
    sharedData!['values'] as SharedData<List<int>, List<int>>,
    sharedData['offset'] as SharedData<int, int>,
  );

  @override
  Map<String, SharedData>? sharedData() => {'values': values, 'offset': offset};

  @override
  SharedData? loadSharedData(String key, dynamic serial) {
    switch (key) {
      case 'values':
        return SharedData<List<int>, List<int>>((serial as List).cast<int>());
      case 'offset':
        return SharedData<int, int>(serial as int);
      default:
        throw StateError('Unknown key: $key');
    }
  }

  @override
  int parameters() => id;

  @override
  FutureOr<int> run() => _sum(values.data) + offset.data + id;
}

class _PingPongTask extends AsyncTask<List<int>, int> {
  final int id;
  final int rounds;
  final AsyncTaskChannelMessageHandler? messageHandler;

  _PingPongTask(this.id, this.rounds, {this.messageHandler});

  @override
  AsyncTask<List<int>, int> instantiate(
    List<int> parameters, [
    Map<String, SharedData>? sharedData,
  ]) => _PingPongTask(parameters[0], parameters[1]);

  @override
  List<int> parameters() => [id, rounds];

  @override
  AsyncTaskChannel? channelInstantiator() =>
      AsyncTaskChannel(messageHandler: messageHandler);

  @override
  FutureOr<int> run() async {
    var channel = channelResolved()!;
    if (!channel.isInExecutionContext) {
      throw StateError('Expected to be in the execution context');
    }

    var sum = 0;
    for (var r = 0; r < rounds; ++r) {
      var m = id * 1000 + r;
      var reply = await channel.sendAndWaitResponse<int, int>(m);
      if (reply != m + 1) {
        throw StateError('Unexpected reply: $reply != ${m + 1}');
      }
      sum += reply;
    }

    var last = await channel.waitMessage<int>();
    channel.send(id * 2);

    return sum + last;
  }
}
