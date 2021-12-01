@Timeout(Duration(seconds: 450))
import 'dart:async';

import 'package:async_task/async_task.dart';
import 'package:async_task/async_task_extension.dart';
import 'package:async_task/src/async_task_channel.dart';
import 'package:test/test.dart';

void main() {
  group('AsyncExecutor', () {
    setUp(() {});

    test(
      'sequential = false, parallelism = 0, withSharedData = true, withTaskChannel = false',
      () => _testParallelism(false, 0, true, false),
      //skip: true,
    );

    test(
      'sequential = false, parallelism = 0, withSharedData = true, withTaskChannel = true',
      () => _testParallelism(false, 0, true, true),
      //skip: true,
    );

    test(
      'sequential = false, parallelism = 0, withSharedData = true, withTaskChannel = true',
      () => _testParallelism(false, 0, false, false),
      //skip: true,
    );

    test(
      'sequential = false, parallelism = 0, withSharedData = true, withTaskChannel = true',
      () => _testParallelism(false, 0, false, true),
      //skip: true,
    );

    // ----------------

    test(
      'sequential = true, parallelism = 0, withSharedData = true, withTaskChannel = false',
      () => _testParallelism(true, 0, true, false),
      //skip: true,
    );

    test(
      'sequential = true, parallelism = 0, withSharedData = true, withTaskChannel = true',
      () => _testParallelism(true, 0, true, true),
      //skip: true,
    );

    // ----------------

    test(
      'sequential = false, parallelism = 1, withSharedData = true, withTaskChannel = false',
      () => _testParallelism(false, 1, true, false),
      //skip: true,
    );

    test(
      'sequential = false, parallelism = 1, withSharedData = true, withTaskChannel = true',
      () => _testParallelism(false, 1, true, true),
      //skip: true,
    );

    // ----------------

    test(
      'sequential = true, parallelism = 1, withSharedData = true, withTaskChannel = false',
      () => _testParallelism(true, 1, true, false),
      //skip: true,
    );

    test(
      'sequential = true, parallelism = 1, withSharedData = true, withTaskChannel = true',
      () => _testParallelism(true, 1, true, true),
      //skip: true,
    );

    // ----------------

    test(
      'sequential = false, parallelism = 2, withSharedData = true, withTaskChannel = false',
      () => _testParallelism(false, 2, true, false),
      //skip: true,
    );

    test(
      'sequential = false, parallelism = 2, withSharedData = true, withTaskChannel = true',
      () => _testParallelism(false, 2, true, true),
      //skip: true,
    );

    test(
      'sequential = false, parallelism = 2, withSharedData = false, withTaskChannel = false',
      () => _testParallelism(false, 2, false, false),
      //skip: true,
    );

    test(
      'sequential = false, parallelism = 2, withSharedData = false, withTaskChannel = true',
      () => _testParallelism(false, 2, false, true),
      //skip: true,
    );

    // ----------------

    test(
      'sequential = true, parallelism = 2, withSharedData = true, withTaskChannel = false',
      () => _testParallelism(true, 2, true, false),
      //skip: true,
    );

    test(
      'sequential = true, parallelism = 2, withSharedData = true, withTaskChannel = true',
      () => _testParallelism(true, 2, true, true),
      //skip: true,
    );

    test(
      'sequential = true, parallelism = 2, withSharedData = false, withTaskChannel = false',
      () => _testParallelism(true, 2, false, false),
      //skip: true,
    );

    test(
      'sequential = true, parallelism = 2, withSharedData = false, withTaskChannel = true',
      () => _testParallelism(true, 2, false, true),
      //skip: true,
    );
  });
}

Future<void> _testParallelism(bool sequential, int parallelism,
    bool withSharedData, bool withTaskChannel) async {
  print('====================================================================');
  var initTime = DateTime.now();
  var executor = await _testParallelismImpl(
      sequential, parallelism, withSharedData, withTaskChannel);
  var endTime = DateTime.now();
  var elapsedTime =
      endTime.millisecondsSinceEpoch - initTime.millisecondsSinceEpoch;
  print(executor);
  await executor.close();

  print('>> Test Time: $elapsedTime');
}

Future<AsyncExecutor> _testParallelismImpl(bool sequential, int parallelism,
    bool withSharedData, bool withTaskChannel) async {
  var executor = AsyncExecutor(
      sequential: sequential,
      parallelism: parallelism,
      taskTypeRegister: _taskRegister);
  print(executor);

  executor.logger.enabled = true;
  executor.logger.enabledExecution = true;

  var counterStartData = withSharedData ? SharedData<int, int>(12000000) : null;
  var counterStartMultiplierData =
      withSharedData ? SharedData<int, int>(2) : null;

  var counterStart = counterStartData?.data ?? 1;
  var counterStartMultiplier = counterStartMultiplierData?.data ?? 1;

  var counters = List<_Counter>.generate(
      10,
      (i) => _Counter(10, i + 1, counterStartData, counterStartMultiplierData,
          withTaskChannel));

  expect(counters.hasIdleTask, isTrue);
  expect(counters.hasTaskWithError, isFalse);
  expect(counters.allTasksSubmitted, isFalse);
  expect(counters.allTasksFinished, isFalse);
  expect(counters.allTasksSuccessful, isFalse);

  expect(counters.idleTasks.length, equals(counters.length));
  expect(counters.submittedTasks.length, equals(0));
  expect(counters.finishedTasks.length, equals(0));
  expect(counters.notFinishedTasks.length, equals(counters.length));
  expect(counters.successfulTasks.length, equals(0));
  expect(counters.errorTasks.length, equals(0));

  var sharedDataInfo = AsyncExecutorSharedDataInfo();
  var executions =
      executor.executeAll(counters, sharedDataInfo: sharedDataInfo);

  expect(executions.length, equals(counters.length));

  for (var i = 0; i < counters.length; ++i) {
    var c = counters[i];
    expect(c.isFinished, isFalse);
    expect(c.result, isNull);
  }

  if (withTaskChannel) {
    // ignore: avoid_function_literals_in_foreach_calls
    counters.forEach((c) async {
      var channel = (await c.channel())!;
      var count = await channel.waitMessage();
      channel.send(count * 3);
    });
  } else {
    // ignore: avoid_function_literals_in_foreach_calls
    counters.forEach((c) async {
      var channel = await c.channel();
      expect(channel, isNull);
    });
  }

  var results = await Future.wait(executions);

  expect(counters.hasIdleTask, isFalse);
  expect(counters.hasTaskWithError, isFalse);
  expect(counters.allTasksSubmitted, isTrue);
  expect(counters.allTasksFinished, isTrue);
  expect(counters.allTasksSuccessful, isTrue);

  expect(counters.idleTasks.length, equals(0));
  expect(counters.submittedTasks.length, equals(counters.length));
  expect(counters.finishedTasks.length, equals(counters.length));
  expect(counters.notFinishedTasks.length, equals(0));
  expect(counters.successfulTasks.length, equals(counters.length));
  expect(counters.errorTasks.length, equals(0));

  expect(sharedDataInfo.sentSharedDataSignatures.length,
      equals(withSharedData && executor.platform.isNative ? 2 : 0));

  await executor.disposeSharedDataInfo(sharedDataInfo);
  print(sharedDataInfo);

  expect(sharedDataInfo.disposedSharedDataSignatures.length,
      equals(sharedDataInfo.sentSharedDataSignatures.length));

  expect(results.length, equals(counters.length));

  print('Counters results: $results');

  var start = counterStart * counterStartMultiplier;

  for (var i = 0; i < counters.length; ++i) {
    var c = counters[i];
    var n = results[i];
    expect(c.isFinished, isTrue);

    var expectedResult = start + c.total * c.stepValue;
    if (withTaskChannel) expectedResult = expectedResult * 3 * 2;

    expect(n, equals(expectedResult));
    expect(n, equals(c.result));

    expect(c.executionTime!.inMilliseconds >= 100, isTrue);
  }

  var listInitTime = counters.map((c) => c.initTime!).toList()..sort();
  var listEndTime = counters.map((c) => c.endTime!).toList()..sort();

  var init = listInitTime.first;
  var end = listEndTime.last;
  var totalTime = end.millisecondsSinceEpoch - init.millisecondsSinceEpoch;

  print('sequential: $sequential');
  print('parallelism: $parallelism');
  print('totalTime: $totalTime');

  var timeDiv = parallelism > 1 ? parallelism : 1;

  expect(totalTime > (!sequential ? 100 : (100 * 10 / timeDiv)), isTrue);
  expect(totalTime < (!sequential ? 100 + 1000 : (100 * 100 / timeDiv) + 1000),
      isTrue);

  await executor.close();

  Object? error;
  try {
    var extraTask = _Counter(
        10, 100, counterStartData, counterStartMultiplierData, withTaskChannel);
    await executor.execute(extraTask);
  } catch (e) {
    error = e;
  }

  expect(error is AsyncExecutorError, isTrue);

  return executor;
}

List<AsyncTask> _taskRegister() =>
    [_Counter(0, 0, SharedData<int, int>(0), SharedData<int, int>(0), false)];

class _Counter extends AsyncTask<List<int>, int> {
  final int total;

  final int stepValue;

  final SharedData<int, int>? start;
  final SharedData<int, int>? startMultiplier;

  final bool withTaskChannel;

  _Counter(this.total, this.stepValue, this.start, this.startMultiplier,
      this.withTaskChannel);

  @override
  AsyncTask<List<int>, int> instantiate(List<int> parameters,
          [Map<String, SharedData>? sharedData]) =>
      _Counter(
          parameters[0],
          parameters[1],
          sharedData?['start'] as SharedData<int, int>?,
          sharedData?['startMultiplier'] as SharedData<int, int>?,
          parameters[2] == 1);

  @override
  Map<String, SharedData>? sharedData() {
    var map = <String, SharedData>{
      if (start != null) 'start': start!,
      if (startMultiplier != null) 'startMultiplier': startMultiplier!
    };
    return map.isNotEmpty ? map : null;
  }

  @override
  SharedData loadSharedData(String key, dynamic serial) {
    switch (key) {
      case 'start':
        return SharedData<int, int>(serial);
      case 'startMultiplier':
        return SharedData<int, int>(serial);
      default:
        throw StateError('Unknown key: $key');
    }
  }

  @override
  List<int> parameters() => [total, stepValue, withTaskChannel ? 1 : 0];

  @override
  AsyncTaskChannel? channelInstantiator() =>
      withTaskChannel ? AsyncTaskChannel() : null;

  @override
  FutureOr<int> run() async {
    var start = this.start?.data ?? 1;
    var startMultiplier = this.startMultiplier?.data ?? 1;

    var count = start * startMultiplier;

    print('$this <<< ...');
    for (var i = 0; i < total; ++i) {
      //print('$this >> $i');
      count += stepValue;
      await Future.delayed(Duration(milliseconds: 10));
    }

    print('$this --- count: $count');

    if (withTaskChannel) {
      var channel = channelResolved()!;

      var result = await channel.sendAndWaitResponse<int, int>(count);

      print('$this --- received result: $count -> $result');

      var result2 = result * 2;

      print('$this >>> final result: $count -> $result -> $result2\t $channel');
      return result2;
    } else {
      var channel = this.channel();
      if (channel != null) {
        throw StateError('Channel should be null: $channel');
      }
      print('$this >>> final result: $count');
      return count;
    }
  }

  @override
  String toString() {
    return '_Counter{total: $total, stepValue: $stepValue}';
  }
}
