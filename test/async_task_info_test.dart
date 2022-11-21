@Timeout(Duration(seconds: 60))
@TestOn('vm')
import 'dart:developer';
import 'dart:isolate';
import 'dart:math' as math;

import 'package:async_task/async_task.dart';
import 'package:async_task/async_task_extension.dart';
import 'package:test/test.dart';
import 'package:vm_service/vm_service.dart' as vm_service;
import 'package:vm_service/vm_service_io.dart';

vm_service.VmService? vmService;

void main() async {
  var serviceInfo = await Service.getInfo();
  var wsUri = serviceInfo.serverWebSocketUri;

  if (wsUri == null) {
    print(
        '** Not running with VM Service! See option `--enable-vm-service`.\n');
  } else {
    vmService = await vmServiceConnectUri(wsUri.toString());
    print('** VM Service: $vmService > $wsUri\n');
  }

  group('AsyncExecutor', () {
    test('info(sequential: false, parallelism: 0)', () async {
      await _testInfo(false, 0);
    });

    test('info(sequential: true, parallelism: 0)', () async {
      await _testInfo(true, 0);
    });

    test('info(sequential: false, parallelism: 1)', () async {
      await _testInfo(false, 1);
    });

    test('info(sequential: true, parallelism: 1)', () async {
      await _testInfo(true, 1);
    });

    test('info(sequential: false, parallelism: 2)', () async {
      await _testInfo(false, 2);
    });

    test('info(sequential: true, parallelism: 2)', () async {
      await _testInfo(true, 2);
    });
  });
}

Future<void> _testInfo(bool sequential, int parallelism) async {
  var asyncExecutor = AsyncExecutor(
      sequential: sequential,
      parallelism: parallelism,
      taskTypeRegister: _taskTypeRegister);

  print(asyncExecutor);

  var totalTasks = 100;

  var tasks = List.generate(totalTasks,
      (init) => _NsTask(_Ns(List.generate(10, (i) => (init * 10) + i))));

  var executions = asyncExecutor.executeAll(tasks);
  var results = await Future.wait(executions);

  for (var init = 0; init < results.length; ++init) {
    var ns = results[init];
    expect(ns.ns, equals(List.generate(10, (i) => ((init * 10) + i) * 3)));
  }

  var info = asyncExecutor.info;

  print(info);
  print(info.threads);

  expect(info.sequential, equals(sequential));
  var expectedWorkers = parallelism > 1 ? parallelism : 1;
  expect(info.maximumWorkers, equals(expectedWorkers));
  expect(info.workers, equals(expectedWorkers));
  expect(info.parallel, equals(parallelism >= 1));

  var tasksPerThread = totalTasks ~/ expectedWorkers;

  var totalDispatchedTasks = 0;
  var totalExecutedTasks = 0;
  for (var i = 0; i < info.threads.length; ++i) {
    var t = info.threads[i];

    totalDispatchedTasks += t.dispatchedTasks;
    totalExecutedTasks += t.executedTasks;

    expect(t.executedTasks, equals(t.dispatchedTasks));

    var margin = math.max(2, tasksPerThread ~/ 2);

    expect(t.dispatchedTasks,
        inInclusiveRange(tasksPerThread - margin, tasksPerThread + margin));
  }

  expect(totalDispatchedTasks, equals(totalTasks));
  expect(totalExecutedTasks, equals(totalTasks));

  executions.clear();
  tasks.clear();
  results.clear();

  if (parallelism > 0) {
    await checkMemoryInstances(vmService, totalTasks, parallelism);
  }

  asyncExecutor.close();
}

List<AsyncTask> _taskTypeRegister() => <AsyncTask>[
      _NsTask(_Ns([0]))
    ];

class _Ns {
  final List<int> ns;

  _Ns(this.ns);

  FutureOr<_Ns> multiply(int m) => _Ns(ns.map((n) => n * m).toList());

  @override
  String toString() {
    return '_Ns$ns';
  }
}

class _NsTask extends AsyncTask<_Ns, _Ns> {
  final _Ns _ns;

  _NsTask(this._ns);

  @override
  AsyncTask<_Ns, _Ns> instantiate(_Ns parameters,
      [Map<String, SharedData>? sharedData]) {
    return _NsTask(parameters);
  }

  @override
  _Ns parameters() => _ns;

  @override
  FutureOr<_Ns> run() {
    return _ns.multiply(3);
  }
}

Future<void> checkMemoryInstances(
    vm_service.VmService? vmService, int totalTasks, int parallelism) async {
  if (vmService == null) return;

  print('-- Checking instances at $vmService');

  var vm = await vmService.getVM();

  var isolate = Isolate.current;
  var isolateID = Service.getIsolateID(isolate)!;

  var vmIsolate = await vmService.getIsolate(isolateID);
  print('-- vmIsolate: ${vmIsolate.name}');

  var vmIsolateGroup = await _resolveIsolateGroup(vm, vmService, isolateID);
  print('-- vmIsolateGroup: ${vmIsolateGroup.id}');

  var isolatesNames = vmIsolateGroup.isolates!.map((e) => e.name).toList();
  print('-- isolatesNames: $isolatesNames');

  var threadsIsolates = vmIsolateGroup.isolates!
      .where((i) => i.name?.contains('async_task/_IsolateThread') ?? false)
      .toList();
  print('-- threadsIsolates: ${threadsIsolates.map((e) => e.name).toList()}');

  expect(threadsIsolates.length, equals(parallelism),
      reason:
          "threadsIsolates:${threadsIsolates.length} != parallelism:$parallelism");

  for (var i in threadsIsolates) {
    var isolateName = i.name;
    var isolateId = i.id!;

    var isolateStr = '$isolateName{$isolateId}';

    var allocationProfile =
        await vmService.getAllocationProfile(isolateId, gc: true);
    print('[$isolateStr] allocationProfile: ${allocationProfile.memoryUsage}');

    var members = allocationProfile.members ?? <vm_service.ClassHeapStats>[];
    var membersAsyncTask = members
        .where((e) => e.classRef?.library?.uri?.contains('async_task') ?? false)
        .toList();

    var memberNs =
        membersAsyncTask.firstWhere((e) => e.classRef?.name == '_Ns');

    var nsInstancesSet =
        await vmService.getInstances(isolateId, memberNs.classRef!.id!, 1000);

    expect(nsInstancesSet.totalCount, lessThan(10));
    expect(nsInstancesSet.totalCount, lessThan(totalTasks ~/ 2));

    print('-- `_Ns` instances: ${nsInstancesSet.totalCount}');

    var nsInstances = nsInstancesSet.instances ?? <vm_service.ObjRef>[];

    for (var objRef in nsInstances) {
      await _showInboundReferences(vmService, isolateId, objRef);
    }
  }

  print('==========================');
}

Future<void> _showInboundReferences(
    vm_service.VmService vmService, String isolateId, vm_service.ObjRef obj,
    {int level = 0, String ident = '', Set<String>? refsIds}) async {
  if (level >= 10) {
    print('$ident ...');
    return;
  }

  refsIds ??= <String>{};

  var objId = obj.id;

  if (objId == null || refsIds.contains(objId)) return;
  refsIds.add(objId);

  var className = obj.json?['class']?['name'] ?? '$obj';

  print('$ident -> $className #${obj.id}');

  if (className == '_Isolate') return;

  var refs = await vmService.getInboundReferences(isolateId, objId, 1000);

  for (var ref in refs.references ?? <vm_service.InboundReference>[]) {
    print(
        '$ident  |-- ${ref.source?.json?['class']?['name']} ${ref.parentField ?? ''}@${ref.parentListIndex ?? ''}');
    var source = ref.source!;
    await _showInboundReferences(vmService, isolateId, source,
        level: level + 1, ident: '  $ident', refsIds: refsIds);
  }
}

Future<vm_service.IsolateGroup> _resolveIsolateGroup(
    vm_service.VM vm, vm_service.VmService vmService, String isolateID) async {
  var isolateGroupsIDs = vm.isolateGroups!.map((e) => e.id!);
  var isolateGroups = await Future.wait(
      isolateGroupsIDs.map((gID) => vmService.getIsolateGroup(gID)));
  var vmIsolateGroup = isolateGroups.firstWhere(
      (g) => g.isolates?.where((i) => i.id == isolateID).isNotEmpty ?? false);
  return vmIsolateGroup;
}
