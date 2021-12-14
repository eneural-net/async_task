@Timeout(Duration(seconds: 450))

import 'package:async_task/async_task.dart';
import 'package:async_task/async_task_extension.dart';
import 'package:test/test.dart';

void main() {
  group('AsyncExecutor', () {
    setUp(() {});

    test(
      'sequential = false, parallelism = 0, sync = false',
      () => _testParallelism(false, 0, false),
      //skip: true,
    );

    test(
      'sequential = false, parallelism = 0, sync = true',
      () => _testParallelism(false, 0, true),
      //skip: true,
    );

    // ----------------

    test(
      'sequential = true, parallelism = 0, sync = false',
      () => _testParallelism(true, 0, false),
      //skip: true,
    );

    test(
      'sequential = true, parallelism = 0, sync = true',
      () => _testParallelism(true, 0, true),
      //skip: true,
    );

    // ----------------

    test(
      'sequential = false, parallelism = 1, sync = false',
      () => _testParallelism(false, 1, false),
      //skip: true,
    );

    test(
      'sequential = false, parallelism = 1, sync = true',
      () => _testParallelism(false, 1, true),
      //skip: true,
    );

    // ----------------

    test(
      'sequential = true, parallelism = 1, sync = false',
      () => _testParallelism(true, 1, false),
      //skip: true,
    );
    test(
      'sequential = true, parallelism = 1, sync = true',
      () => _testParallelism(true, 1, true),
      //skip: true,
    );

    // ----------------

    test(
      'sequential = false, parallelism = 2, sync = false',
      () => _testParallelism(false, 2, false),
      //skip: true,
    );
    test(
      'sequential = false, parallelism = 2, sync = true',
      () => _testParallelism(false, 2, true),
      //skip: true,
    );
    // ----------------

    test(
      'sequential = true, parallelism = 2, sync = false',
      () => _testParallelism(true, 2, false),
      //skip: true,
    );

    test(
      'sequential = true, parallelism = 2, sync = true',
      () => _testParallelism(true, 2, true),
      //skip: true,
    );
  });
}

Future<void> _testParallelism(
    bool sequential, int parallelism, bool sync) async {
  print('====================================================================');
  var initTime = DateTime.now();
  var executor = await _testParallelismImpl(sequential, parallelism, sync);
  var endTime = DateTime.now();
  var elapsedTime =
      endTime.millisecondsSinceEpoch - initTime.millisecondsSinceEpoch;
  print(executor);
  await executor.close();

  print('>> Test Time: $elapsedTime');
}

Future<AsyncExecutor> _testParallelismImpl(
    bool sequential, int parallelism, bool sync) async {
  var executor = AsyncExecutor(
      sequential: sequential,
      parallelism: parallelism,
      taskTypeRegister: _taskRegister);
  print(executor);

  executor.logger.enabled = true;
  executor.logger.enabledExecution = true;

  var echoes = List<_Echo>.generate(10, (i) => _Echo(i + 1, sync));

  expect(echoes.hasIdleTask, isTrue);
  expect(echoes.hasTaskWithError, isFalse);
  expect(echoes.allTasksSubmitted, isFalse);
  expect(echoes.allTasksFinished, isFalse);
  expect(echoes.allTasksSuccessful, isFalse);

  expect(echoes.idleTasks.length, equals(echoes.length));
  expect(echoes.submittedTasks.length, equals(0));
  expect(echoes.finishedTasks.length, equals(0));
  expect(echoes.notFinishedTasks.length, equals(echoes.length));
  expect(echoes.successfulTasks.length, equals(0));
  expect(echoes.errorTasks.length, equals(0));

  var executions = executor.executeAll(echoes);

  expect(executions.length, equals(echoes.length));

  for (var i = 0; i < echoes.length; ++i) {
    var c = echoes[i];
    expect(c.isFinished, isFalse);
    expect(c.result, isNull);
  }

  for (var e in echoes) {
    var channel = (await e.channel())!;

    for (var i = 0; i < 10; ++i) {
      print('[$e] send> i');
      var r = await channel.sendAndWaitResponse(i);
      print('[$e] response> i');
      expect(r, equals(i * e.multiplier));
    }

    channel.send(-1);
  }

  var results = await Future.wait(executions);

  expect(echoes.hasIdleTask, isFalse);
  expect(echoes.hasTaskWithError, isFalse);
  expect(echoes.allTasksSubmitted, isTrue);
  expect(echoes.allTasksFinished, isTrue);
  expect(echoes.allTasksSuccessful, isTrue);

  expect(echoes.idleTasks.length, equals(0));
  expect(echoes.submittedTasks.length, equals(echoes.length));
  expect(echoes.finishedTasks.length, equals(echoes.length));
  expect(echoes.notFinishedTasks.length, equals(0));
  expect(echoes.successfulTasks.length, equals(echoes.length));
  expect(echoes.errorTasks.length, equals(0));

  expect(results.length, equals(echoes.length));

  print('Echos results: $results');

  for (var i = 0; i < echoes.length; ++i) {
    var e = echoes[i];
    var res = results[i];
    expect(e.isFinished, isTrue);

    var expectedResult = List.generate(10, (i) => i * e.multiplier);

    expect(res, equals(expectedResult));
    expect(res, equals(e.result));
  }

  var listInitTime = echoes.map((c) => c.initTime!).toList()..sort();
  var listEndTime = echoes.map((c) => c.endTime!).toList()..sort();

  var init = listInitTime.first;
  var end = listEndTime.last;
  var totalTime = end.millisecondsSinceEpoch - init.millisecondsSinceEpoch;

  print('sequential: $sequential');
  print('parallelism: $parallelism');
  print('totalTime: $totalTime');

  await executor.close();

  Object? error;
  try {
    var extraTask = _Echo(10, false);
    await executor.execute(extraTask);
  } catch (e) {
    error = e;
  }

  expect(error is AsyncExecutorError, isTrue);

  return executor;
}

List<AsyncTask> _taskRegister() => [_Echo(1, false)];

class _Echo extends AsyncTask<List<int>, List<int>> {
  final int multiplier;
  final bool sync;

  _Echo(this.multiplier, this.sync);

  @override
  AsyncTask<List<int>, List<int>> instantiate(List<int> parameters,
          [Map<String, SharedData>? sharedData]) =>
      _Echo(parameters[0], parameters[1] == 1);

  @override
  List<int> parameters() => [multiplier, sync ? 1 : 0];

  @override
  AsyncTaskChannel? channelInstantiator() => AsyncTaskChannel();

  final List<int> _processedMessages = <int>[];

  @override
  FutureOr<List<int>> run() async {
    print('$this <<< ...');

    var channel = channelResolved()!;

    while (true) {
      int m;

      if (sync) {
        while (channel.messageQueueIsEmpty) {
          print('-- sleep...');
          await Future.delayed(Duration(milliseconds: 1));
        }

        print('-- [$this]> readMessage...');
        m = channel.readMessage<int>()!;
      } else {
        print('-- [$this]> waitMessage...');
        m = await channel.waitMessage<int>();
      }

      print('-- [$this]> message: $m');

      if (m >= 0) {
        var m2 = m * multiplier;
        channel.send(m2);
        _processedMessages.add(m2);
      } else {
        print('$this >>> final result: $_processedMessages');
        return _processedMessages;
      }
    }
  }

  @override
  String toString() {
    return '_Echo{multiplier: $multiplier, sync: $sync}';
  }
}
