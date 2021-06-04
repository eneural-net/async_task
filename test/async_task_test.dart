@Timeout(Duration(seconds: 45))
import 'dart:async';

import 'package:async_task/async_task.dart';
import 'package:test/test.dart';

void main() {
  group('AsyncExecutor', () {
    setUp(() {});

    test(
      'sequential = false, parallelism = 0',
      () => _testParallelism(false, 0),
      //skip: true,
    );

    test(
      'sequential = true, parallelism = 0',
      () => _testParallelism(true, 0),
      //skip: true,
    );

    test(
      'sequential = false, parallelism = 1',
      () => _testParallelism(false, 1),
      //skip: true,
    );

    test(
      'sequential = true, parallelism = 1',
      () => _testParallelism(true, 1),
      //skip: true,
    );

    test(
      'sequential = false, parallelism = 2',
      () => _testParallelism(false, 2),
      //skip: true,
    );

    test(
      'sequential = true, parallelism = 2',
      () => _testParallelism(true, 2),
      //skip: true,
    );
  });
}

Future<void> _testParallelism(bool sequential, int parallelism) async {
  var initTime = DateTime.now();
  var executor = await _testParallelismImpl(sequential, parallelism);
  var endTime = DateTime.now();
  var elapsedTime =
      endTime.millisecondsSinceEpoch - initTime.millisecondsSinceEpoch;
  print(executor);
  await executor.close();

  print('Test Time: $elapsedTime');
}

Future<AsyncExecutor> _testParallelismImpl(
    bool sequential, int parallelism) async {
  print('====================================================================');

  var executor = AsyncExecutor(
      sequential: sequential,
      parallelism: parallelism,
      taskTypeRegister: _taskRegister);
  print(executor);

  executor.logger.enabled = true;

  var counterStart = SharedData<int, int>(12000000);
  var counterStartMultiplier = SharedData<int, int>(2);

  var counters = List<_Counter>.generate(
      10, (i) => _Counter(10, i + 1, counterStart, counterStartMultiplier));

  var executions = executor.executeAll(counters);

  expect(executions.length, equals(counters.length));

  for (var i = 0; i < counters.length; ++i) {
    var c = counters[i];
    expect(c.isFinished, isFalse);
    expect(c.result, isNull);
  }

  var results = await Future.wait(executions);

  expect(results.length, equals(counters.length));

  print('Counters results: $results');

  var start = counterStart.data * counterStartMultiplier.data;

  for (var i = 0; i < counters.length; ++i) {
    var c = counters[i];
    var n = results[i];
    expect(c.isFinished, isTrue);
    expect(n, equals(start + c.total * c.stepValue));
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

  var error;
  try {
    var extraTask = _Counter(10, 100, counterStart, counterStartMultiplier);
    await executor.execute(extraTask);
  } catch (e) {
    error = e;
  }

  expect(error is AsyncExecutorError, isTrue);

  return executor;
}

List<AsyncTask> _taskRegister() =>
    [_Counter(0, 0, SharedData<int, int>(0), SharedData<int, int>(0))];

class _Counter extends AsyncTask<List<int>, int> {
  final int total;

  final int stepValue;

  final SharedData<int, int> start;
  final SharedData<int, int> startMultiplier;

  _Counter(this.total, this.stepValue, this.start, this.startMultiplier);

  @override
  AsyncTask<List<int>, int> instantiate(List<int> parameters,
          [Map<String, SharedData>? sharedData]) =>
      _Counter(
          parameters[0],
          parameters[1],
          sharedData!['start'] as SharedData<int, int>,
          sharedData['startMultiplier'] as SharedData<int, int>);

  @override
  Map<String, SharedData> sharedData() =>
      <String, SharedData>{'start': start, 'startMultiplier': startMultiplier};

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
  List<int> parameters() => <int>[total, stepValue];

  @override
  FutureOr<int> run() async {
    var count = start.data * startMultiplier.data;

    print('$this <<< ...');
    for (var i = 0; i < total; ++i) {
      //print('$this >> $i');
      count += stepValue;
      await Future.delayed(Duration(milliseconds: 10));
    }
    print('$this >>> $count');

    return count;
  }

  @override
  String toString() {
    return '_Counter{total: $total, stepValue: $stepValue}';
  }
}
