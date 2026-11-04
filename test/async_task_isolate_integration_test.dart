@TestOn('vm')
@Timeout(Duration(seconds: 120))
library;

import 'dart:developer';
import 'dart:isolate';

import 'package:async_task/async_task.dart';
import 'package:async_task/async_task_extension.dart';
import 'package:test/test.dart';
import 'package:vm_service/vm_service.dart' as vm_service;
import 'package:vm_service/vm_service_io.dart';

/// Integration tests of the `Isolate` executor: `SharedData` transfer,
/// failures while loading it, dispose and isolates lifecycle.
void main() {
  group('SharedData (Isolate)', () {
    late AsyncExecutor executor;

    setUp(() {
      executor = AsyncExecutor(
        parallelism: 1,
        taskTypeRegister: _taskTypeRegister,
      );
    });

    tearDown(() => executor.close());

    test('a failing loadSharedData completes the task with an error', () async {
      var sd = SharedData<int, int>(7);

      // `loadSharedData` throws on the 1st and 2nd calls (in the isolate):
      await expectLater(
        executor.execute(_LoadTask(1, {'fail2': sd})),
        throwsA(
          isA<AsyncExecutorError>()
              .having(
                (e) => e.message,
                'message',
                startsWith('AsyncTask execution error at _IsolateThread#'),
              )
              .having(
                (e) => e.cause,
                'cause',
                isA<StateError>().having(
                  (e) => e.message,
                  'message',
                  equals('load failure #1'),
                ),
              ),
        ),
      );

      // The `SharedData` was already sent to the isolate (from the main side
      // point of view), but it's missing in the isolate: the isolate requests
      // it from the main side. The 2nd load also fails:
      await expectLater(
        executor.execute(_LoadTask(2, {'fail2': sd})),
        throwsA(
          isA<AsyncExecutorError>().having(
            (e) => e.cause,
            'cause',
            isA<StateError>().having(
              (e) => e.message,
              'message',
              equals('load failure #2'),
            ),
          ),
        ),
      );

      // Requested again, now successfully (3rd load):
      expect(
        await executor.execute(_LoadTask(3, {'fail2': sd})),
        equals([3 * 7, 3]),
      );

      // Now present in the isolate: no new load.
      expect(
        await executor.execute(_LoadTask(4, {'fail2': sd})),
        equals([4 * 7, 3]),
      );

      expect(executor.info.threads.single.executedTasks, equals(4));
    });

    test('concurrent tasks requesting the same missing SharedData', () async {
      var sd = SharedData<int, int>(5);

      await expectLater(
        executor.execute(_LoadTask(1, {'fail1': sd})),
        throwsA(isA<AsyncExecutorError>()),
      );

      // All the tasks are in the isolate before the requested `SharedData`
      // arrives: only one request/load is performed.
      var results = await Future.wait([
        for (var i = 0; i < 20; ++i)
          executor.execute(_LoadTask(i, {'fail1': sd})),
      ]);

      expect(
        results,
        equals([
          for (var i = 0; i < 20; ++i) [i * 5, 2],
        ]),
      );
    });

    test('loadSharedData returning null', () async {
      await expectLater(
        executor.execute(_NullLoadTask(SharedData<int, int>(1))),
        throwsA(
          isA<AsyncExecutorError>().having(
            (e) => e.cause,
            'cause',
            isA<TypeError>(),
          ),
        ),
      );

      // The isolate is still usable:
      expect(
        await executor.execute(_LoadTask(2, {'a': SharedData<int, int>(3)})),
        equals([6, 1]),
      );
    });

    test('not registered task type with SharedData', () async {
      await expectLater(
        executor.execute(_NotRegisteredSharedDataTask(SharedData<int, int>(1))),
        throwsA(
          isA<AsyncExecutorError>().having(
            (e) => e.cause,
            'cause',
            isA<StateError>().having(
              (e) => e.message,
              'message',
              contains("Can't find registered task"),
            ),
          ),
        ),
      );

      expect(
        await executor.execute(_LoadTask(3, {'a': SharedData<int, int>(3)})),
        equals([9, 1]),
      );
    });

    test('same SharedData content in two keys of a task', () async {
      // Same signature: sent once, loaded once and used for both keys.
      var a = SharedData<int, int>(10);
      var b = SharedData<int, int>(10);
      expect(a.signature, equals(b.signature));

      var sharedDataInfo = AsyncExecutorSharedDataInfo();
      expect(
        await executor.execute(
          _LoadTask(2, {'a': a, 'b': b}),
          sharedDataInfo: sharedDataInfo,
        ),
        equals([2 * 20, 1]),
      );
      expect(sharedDataInfo.sentSharedDataSignatures, equals({a.signature}));
    });

    test('already sent and new SharedData in the same task', () async {
      var a = SharedData<int, int>(2);
      var b = SharedData<int, int>(3);

      expect(await executor.execute(_LoadTask(1, {'a': a})), equals([2, 1]));
      expect(
        await executor.execute(_LoadTask(1, {'a': a, 'b': b})),
        equals([5, 2]),
      );
      expect(
        await executor.execute(_LoadTask(1, {'b': b, 'a': a})),
        equals([5, 2]),
      );
    });

    test('dispose and reuse SharedData', () async {
      var sd = SharedData<int, int>(4);

      var info1 = AsyncExecutorSharedDataInfo();
      expect(
        await executor.execute(_LoadTask(1, {'a': sd}), sharedDataInfo: info1),
        equals([4, 1]),
      );
      expect(info1.sentSharedDataSignatures, equals({sd.signature}));

      // Already in the isolate:
      expect(await executor.execute(_LoadTask(2, {'a': sd})), equals([8, 1]));

      expect(await executor.disposeSharedDataInfo(info1), isTrue);
      expect(info1.disposedSharedDataSignatures, equals({sd.signature}));

      // Sent and loaded again:
      expect(await executor.execute(_LoadTask(3, {'a': sd})), equals([12, 2]));

      expect(await executor.disposeSharedData({sd.signature}), isTrue);
      expect(await executor.execute(_LoadTask(4, {'a': sd})), equals([16, 3]));
    });

    test('async dispose between queued tasks', () async {
      var sd = SharedData<int, int>(6);
      expect(await executor.execute(_LoadTask(1, {'a': sd})), equals([6, 1]));

      // Messages to the isolate keep their order: task, dispose, task.
      var f1 = executor.execute(_LoadTask(2, {'a': sd}));
      executor.disposeSharedData({sd.signature}, async: true);
      var f2 = executor.execute(_LoadTask(3, {'a': sd}));
      var f3 = executor.execute(_LoadTask(4, {'a': sd}));

      expect(await f1, equals([12, 1]));
      // Re-sent after the dispose:
      expect(await f2, equals([18, 2]));
      expect(await f3, equals([24, 2]));
    });

    test('error with a cause that cannot be sent between isolates', () async {
      await expectLater(
        executor.execute(_UnsendableExecutorErrorTask()),
        throwsA(
          isA<AsyncExecutorError>()
              .having((e) => e.message, 'message', equals('custom error'))
              // Falls back to the `toString` of the cause:
              .having((e) => e.cause, 'cause', equals('_Unsendable')),
        ),
      );
    });
  });

  group('SharedData under load (Isolate)', () {
    test('many tasks with new SharedData submitted concurrently', () async {
      var executor = AsyncExecutor(
        parallelism: 4,
        taskTypeRegister: _taskTypeRegister,
      );

      try {
        var sharedDatas = [
          for (var i = 0; i < 10; ++i) SharedData<int, int>(1000 + i),
        ];

        const total = 300;
        var sharedDataInfo = AsyncExecutorSharedDataInfo();
        var results = await executor.executeAllAndWaitResults([
          for (var i = 0; i < total; ++i)
            _LoadTask(i, {'a': sharedDatas[i % sharedDatas.length]}),
        ], sharedDataInfo: sharedDataInfo);

        for (var i = 0; i < total; ++i) {
          var data = 1000 + (i % sharedDatas.length);
          expect(results[i][0], equals(i * data), reason: 'task #$i');
          // Each isolate loads each `SharedData` at most once:
          expect(
            results[i][1],
            inInclusiveRange(1, sharedDatas.length),
            reason: 'task #$i',
          );
        }

        expect(
          sharedDataInfo.sentSharedDataSignatures,
          equals(sharedDatas.map((e) => e.signature).toSet()),
        );

        var loadsPerIsolate = await Future.wait([
          for (var i = 0; i < executor.info.workers * 4; ++i)
            executor.execute(_LoadCountTask()),
        ]);
        for (var loads in loadsPerIsolate) {
          expect(loads, lessThanOrEqualTo(sharedDatas.length));
        }
      } finally {
        await executor.close();
      }
    });
  });

  group('AsyncExecutor lifecycle (Isolate)', () {
    test('isInExecutionContext of a task from another executor', () async {
      var executor1 = AsyncExecutor(
        parallelism: 1,
        taskTypeRegister: _taskTypeRegister,
      );
      var executor2 = AsyncExecutor(
        parallelism: 1,
        taskTypeRegister: _taskTypeRegister,
      );

      try {
        var t1 = _LoadTask(1, {'a': SharedData<int, int>(1)});
        var t2 = _LoadTask(2, {'a': SharedData<int, int>(1)});
        await executor1.execute(t1);
        await executor2.execute(t2);

        expect(t1.isInExecutionContext, isFalse);
        expect(
          () => t1.executorThread!.isInExecutionContext(t2),
          throwsStateError,
        );
      } finally {
        await executor1.close();
        await executor2.close();
      }
    });

    test('isolates are terminated on close', () async {
      var vmService = await _connectVmService();
      if (vmService == null) {
        markTestSkipped('VM Service not available');
        return;
      }

      try {
        for (var round = 0; round < 3; ++round) {
          var executor = AsyncExecutor(
            parallelism: 4,
            taskTypeRegister: _taskTypeRegister,
          );

          var results = await executor.executeAllAndWaitResults([
            for (var i = 0; i < 100; ++i)
              _LoadTask(i, {'a': SharedData<int, int>(i % 3)}),
          ]);
          expect(results.length, equals(100));

          var names = executor.info.threads
              .map((t) => 'async_task/_IsolateThread/${t.id}')
              .toSet();
          expect(names.length, equals(executor.info.workers));
          expect(names, isNotEmpty);

          var alive = await _aliveIsolates(vmService, names);
          expect(alive, equals(names), reason: 'round $round');

          await executor.close();
          expect(executor.info.workers, equals(0));

          // The isolates are killed asynchronously after confirming the close:
          var stillAlive = await _waitIsolatesTermination(vmService, names);
          expect(stillAlive, isEmpty, reason: 'round $round');
        }

        // No executor isolate left in this isolate group:
        var leftovers = await _aliveIsolates(vmService, null);
        expect(leftovers, isEmpty);
      } finally {
        await vmService.dispose();
      }
    });
  });
}

Future<vm_service.VmService?> _connectVmService() async {
  var info = await Service.getInfo();
  var uri = info.serverWebSocketUri;
  if (uri == null) {
    info = await Service.controlWebServer(enable: true, silenceOutput: true);
    uri = info.serverWebSocketUri;
  }
  if (uri == null) return null;
  return vmServiceConnectUri(uri.toString());
}

/// Returns the names of the alive executor isolates in the current isolate
/// group. If [names] is provided, only those names are considered.
Future<Set<String>> _aliveIsolates(
  vm_service.VmService vmService,
  Set<String>? names,
) async {
  var isolateId = Service.getIsolateId(Isolate.current)!;
  var vm = await vmService.getVM();

  for (var groupRef in vm.isolateGroups ?? <vm_service.IsolateGroupRef>[]) {
    var group = await vmService.getIsolateGroup(groupRef.id!);
    var isolates = group.isolates ?? <vm_service.IsolateRef>[];
    if (!isolates.any((i) => i.id == isolateId)) continue;

    return isolates
        .map((i) => i.name ?? '')
        .where((n) => n.startsWith('async_task/_IsolateThread/'))
        .where((n) => names == null || names.contains(n))
        .toSet();
  }

  throw StateError("Can't find the current isolate group");
}

/// Polls (up to a generous deadline) until the [names] isolates terminate.
/// Returns the isolates still alive.
Future<Set<String>> _waitIsolatesTermination(
  vm_service.VmService vmService,
  Set<String> names,
) async {
  var deadline = DateTime.now().add(Duration(seconds: 30));
  var alive = await _aliveIsolates(vmService, names);
  while (alive.isNotEmpty && DateTime.now().isBefore(deadline)) {
    await Future.delayed(Duration(milliseconds: 20));
    alive = await _aliveIsolates(vmService, names);
  }
  return alive;
}

List<AsyncTask> _taskTypeRegister() => <AsyncTask>[
  _LoadTask(0, {}),
  _NullLoadTask(SharedData<int, int>(0)),
  _UnsendableExecutorErrorTask(),
  _LoadCountTask(),
];

/// `SharedData` loads performed in the current isolate.
int _loads = 0;

/// A task that multiplies its parameter by the sum of its `SharedData`.
///
/// The result is `[parameter * sum, loads]`, where `loads` is the number of
/// `SharedData` loads performed in the executing isolate.
///
/// - A key `failN` makes the first `N` loads (in the isolate) fail.
///   Note that [loadSharedData] is called on the registered task instance,
///   so the failure can't be configured by a task field.
class _LoadTask extends AsyncTask<int, List<int>> {
  final int n;
  final Map<String, SharedData> sharedDatas;

  _LoadTask(this.n, this.sharedDatas);

  @override
  AsyncTask<int, List<int>> instantiate(
    int parameters, [
    Map<String, SharedData>? sharedData,
  ]) => _LoadTask(parameters, sharedData ?? {});

  @override
  Map<String, SharedData>? sharedData() => sharedDatas;

  @override
  SharedData? loadSharedData(String key, dynamic serial) {
    var load = ++_loads;
    if (key.startsWith('fail') && load <= int.parse(key.substring(4))) {
      throw StateError('load failure #$load');
    }
    return SharedData<int, int>(serial as int);
  }

  @override
  int parameters() => n;

  @override
  FutureOr<List<int>> run() {
    var sum = sharedDatas.values.fold<int>(0, (s, e) => s + (e.data as int));
    return [n * sum, _loads];
  }
}

/// Returns the number of `SharedData` loads in the executing isolate.
class _LoadCountTask extends AsyncTask<int, int> {
  @override
  AsyncTask<int, int> instantiate(
    int parameters, [
    Map<String, SharedData>? sharedData,
  ]) => _LoadCountTask();

  @override
  int parameters() => 0;

  @override
  FutureOr<int> run() => Future.delayed(Duration.zero, () => _loads);
}

class _NullLoadTask extends AsyncTask<int, int> {
  final SharedData<int, int> sd;

  _NullLoadTask(this.sd);

  @override
  AsyncTask<int, int> instantiate(
    int parameters, [
    Map<String, SharedData>? sharedData,
  ]) => _NullLoadTask(sharedData!['sd'] as SharedData<int, int>);

  @override
  Map<String, SharedData>? sharedData() => {'sd': sd};

  // Not overriding `loadSharedData`: the default returns `null`.

  @override
  int parameters() => 0;

  @override
  FutureOr<int> run() => sd.data;
}

/// Not present in [_taskTypeRegister].
class _NotRegisteredSharedDataTask extends _NullLoadTask {
  _NotRegisteredSharedDataTask(super.sd);
}

class _Unsendable {
  // A `RawReceivePort` can't be sent to another isolate:
  final RawReceivePort port;

  _Unsendable(this.port);

  @override
  String toString() => '_Unsendable';
}

class _UnsendableExecutorErrorTask extends AsyncTask<int, int> {
  @override
  AsyncTask<int, int> instantiate(
    int parameters, [
    Map<String, SharedData>? sharedData,
  ]) => _UnsendableExecutorErrorTask();

  @override
  int parameters() => 0;

  @override
  FutureOr<int> run() {
    var port = RawReceivePort()..close();
    throw AsyncExecutorError('custom error', _Unsendable(port));
  }
}
