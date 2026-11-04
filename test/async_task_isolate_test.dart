@TestOn('vm')
@Timeout(Duration(seconds: 120))
library;

import 'dart:async';
import 'dart:isolate';

import 'package:async_task/async_task.dart';
import 'package:async_task/async_task_shared_pointer.dart';
import 'package:test/test.dart';

/// Integration tests specific to the `Isolate` backed executor.
void main() {
  group('AsyncExecutor (Isolate)', () {
    test('requires a taskTypeRegister', () {
      expect(
        () => AsyncExecutor(parallelism: 1),
        throwsA(
          isA<StateError>().having(
            (e) => e.message,
            'message',
            contains('taskRegister'),
          ),
        ),
      );
    });

    test('platform and info', () async {
      var executor = AsyncExecutor(
        parallelism: 1,
        taskTypeRegister: _taskTypeRegister,
      );

      try {
        expect(executor.platform.isNative, isTrue);
        expect(
          executor.platform.maximumParallelism,
          equals(AsyncExecutor.maximumParallelism),
        );
        expect(executor.maximumWorkers, equals(1));

        expect(await executor.execute(_MulTask(6, 7)), equals(42));

        var info = executor.info;
        expect(info.parallel, isTrue);
        expect(info.workers, equals(1));
        expect(info.threads.single.executedTasks, equals(1));
      } finally {
        await executor.close();
      }
    });

    test('async taskTypeRegister', () async {
      var executor = AsyncExecutor(
        parallelism: 1,
        taskTypeRegister: _taskTypeRegisterAsync,
      );

      try {
        var results = await executor.executeAllAndWaitResults([
          _MulTask(2, 3),
          _MulTask(4, 5),
        ]);
        expect(results, equals([6, 20]));
      } finally {
        await executor.close();
      }
    });

    test('not registered task type', () async {
      var executor = AsyncExecutor(
        parallelism: 1,
        taskTypeRegister: _taskTypeRegister,
      );

      try {
        await expectLater(
          executor.execute(_NotRegisteredTask()),
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
                    contains("Can't find registered task"),
                  ),
                ),
          ),
        );

        // The isolate is still usable:
        expect(await executor.execute(_MulTask(3, 3)), equals(9));
      } finally {
        await executor.close();
      }
    });

    test('error that cannot be sent between isolates', () async {
      var executor = AsyncExecutor(
        parallelism: 1,
        taskTypeRegister: _taskTypeRegister,
      );

      try {
        await expectLater(
          executor.execute(_UnsendableErrorTask()),
          throwsA(
            isA<AsyncExecutorError>()
                .having(
                  (e) => e.message,
                  'message',
                  startsWith('AsyncTask execution error'),
                )
                .having(
                  (e) => e.cause,
                  'cause',
                  // Falls back to the `toString` of the original error:
                  isA<String>().having(
                    (e) => e,
                    'string',
                    contains('_UnsendableError'),
                  ),
                ),
          ),
        );

        // The isolate is still usable:
        expect(await executor.execute(_MulTask(2, 2)), equals(4));
      } finally {
        await executor.close();
      }
    });

    test('close without starting', () async {
      var executor = AsyncExecutor(
        parallelism: 1,
        taskTypeRegister: _taskTypeRegister,
      );
      expect(await executor.close(), isTrue);
      expect(executor.isClosed, isTrue);
      expect(executor.info.workers, equals(0));
    });

    test('isolates are released on close', () async {
      var executor = AsyncExecutor(
        parallelism: 2,
        taskTypeRegister: _taskTypeRegister,
      );

      var results = await executor.executeAllAndWaitResults([
        for (var i = 0; i < 8; ++i) _MulTask(i, 2),
      ]);
      expect(results, equals([for (var i = 0; i < 8; ++i) i * 2]));
      expect(executor.info.workers, greaterThanOrEqualTo(1));

      await executor.close();
      expect(executor.info.workers, equals(0));
    });

    test('SharedPointer as SharedData', () async {
      var executor = AsyncExecutor(
        parallelism: 1,
        taskTypeRegister: _taskTypeRegister,
      );

      var pointer = SharedPointerUInt32List(4);
      try {
        for (var i = 0; i < 4; ++i) {
          pointer.writeAt(i, (i + 1) * 10);
        }

        var sum = await executor.execute(
          _PointerSumTask(
            SharedData(pointer, noSerializationOnDartNative: true),
          ),
        );
        expect(sum, equals(100));
      } finally {
        await executor.close();
        pointer.free();
      }
    });
  });

  group('SharedPointerAddress', () {
    test('serialize / deserialize', () {
      var pointer = SharedPointerUint32();
      try {
        var address = pointer.address;

        var serial = address.serialize();
        var address2 = address.deserialize(serial);
        expect(address2, equals(address));
        expect(address2.type, equals(address.type));
        expect(address2.parameters, equals(address.parameters));
        expect(address2.hashCode, equals(address.hashCode));

        var address3 = address.deserializeData(serial);
        expect(address3, equals(address));

        var copy = address.copy();
        expect(copy, equals(address));
        expect(identical(copy.parameters, address.parameters), isFalse);

        expect(identical(address.instantiate(address), address), isTrue);
        expect(address.requiresSerializationToBeShared, isFalse);
        expect('$address', startsWith('SharedPointerAddress{ #'));
        expect(address.computeSignature(), equals(address2.computeSignature()));
      } finally {
        pointer.free();
      }
    });

    test('getPointer errors', () {
      expect(
        () => SharedPointerAddress(0).getPointer(),
        throwsA(isA<StateError>()),
      );
      expect(
        () => SharedPointerAddress(0, String).getPointer(),
        throwsA(isA<StateError>()),
      );
    });
  });

  group('SharedPointer', () {
    test('SharedPointerMapped', () {
      var pointer = SharedPointerMapped<int>(
        8,
        2,
        (byteData, offset, value) => byteData.setInt16(offset, value),
        (byteData, offset) => byteData.getInt16(offset),
      );

      try {
        expect(pointer.length, equals(4));
        expect(pointer.isEmpty, isFalse);
        expect(pointer.isNotEmpty, isTrue);
        expect(pointer.isInitializer, isTrue);
        expect(pointer.requiresSerializationToBeShared, isTrue);

        pointer.write(-7);
        pointer.writeAt(3, 1234);
        expect(pointer.read(), equals(-7));
        expect(pointer.readAt(3), equals(1234));

        var copy = pointer.copy();
        expect(copy.isInitializer, isFalse);
        expect(copy.address, equals(pointer.address));
        expect(copy.read(), equals(-7));
        expect(copy.readAt(3), equals(1234));

        expect(identical(pointer.instantiate(copy), copy), isTrue);
        expect(pointer.serialize(), equals(pointer.address));
        expect(pointer.computeSignature(), startsWith('SharedPointer>'));

        // Not supported automatically (needs the reader/writer functions):
        expect(() => pointer.deserialize(pointer.address), throwsStateError);
        expect(
          () => pointer.deserializeData(pointer.address),
          throwsStateError,
        );

        // A copy doesn't free the memory:
        copy.free();
        expect(copy.isClosed, isTrue);
        expect(() => copy.read(), throwsStateError);
        expect(pointer.read(), equals(-7));
      } finally {
        pointer.free();
      }

      expect(pointer.isClosed, isTrue);
      expect(() => pointer.checkClosed(), throwsStateError);
      // Already freed: no effect.
      pointer.free();
    });

    test('copy / deserialize of typed pointers', () {
      void check<P extends SharedPointerBytes<int>>(P pointer) {
        try {
          pointer.write(5);

          var copy = pointer.copy() as P;
          expect(copy.isInitializer, isFalse);
          expect(copy.read(), equals(5));

          var deserialized = pointer.deserialize(pointer.serialize()) as P;
          expect(deserialized.read(), equals(5));

          var deserializedData =
              pointer.deserializeData(pointer.serialize()) as P;
          expect(deserializedData.read(), equals(5));

          expect(identical(pointer.instantiate(copy), copy), isTrue);
        } finally {
          pointer.free();
        }
      }

      check(SharedPointerUint32());
      check(SharedPointerUint64());
      check(SharedPointerUInt8List(4));
      check(SharedPointerUInt32List(4));
      check(SharedPointerUInt64List(4));
    });

    test('views and sublists', () {
      var p8 = SharedPointerUInt8List(4);
      var p32 = SharedPointerUInt32List(4);
      var p64 = SharedPointerUInt64List(4);

      try {
        for (var i = 0; i < 4; ++i) {
          p8.writeAt(i, i + 1);
          p32.writeAt(i, (i + 1) * 100);
          p64.writeAt(i, (i + 1) * 1000);
        }

        expect(p8.view(), equals([1, 2, 3, 4]));
        expect(p8.sublist(1), equals([2, 3, 4]));
        expect(p8.sublist(1, 2), equals([2]));
        expect(p8.bytes.length, equals(4));
        expect(p8.byteData.lengthInBytes, equals(4));

        expect(p32.view(), equals([100, 200, 300, 400]));
        expect(p32.sublist(2), equals([300, 400]));

        expect(p64.view(), equals([1000, 2000, 3000, 4000]));
        expect(p64.sublist(0, 2), equals([1000, 2000]));
      } finally {
        p8.free();
        p32.free();
        p64.free();
      }
    });

    test('readNewValue', () async {
      var pointer = SharedPointerUint32();
      try {
        pointer.write(1);

        // A new value is already present:
        expect(await pointer.readNewValue(lastValue: 0), equals(1));
        expect(await pointer.readNewValue(), equals(1));

        // Value written by another isolate while polling:
        var address = pointer.address;
        var future = pointer.readNewValue(
          lastValue: 1,
          interval: Duration(milliseconds: 1),
          timeout: Duration(seconds: 30),
        );
        await Isolate.run(() {
          var p = address.getPointer<int>();
          p.write(2);
        });
        expect(await future, equals(2));
      } finally {
        pointer.free();
      }
    });
  });
}

List<AsyncTask> _taskTypeRegister() => <AsyncTask>[
  _MulTask(0, 0),
  _UnsendableErrorTask(),
  _PointerSumTask(SharedData(SharedPointerUInt32List(0))),
];

Future<List<AsyncTask>> _taskTypeRegisterAsync() async => _taskTypeRegister();

class _MulTask extends AsyncTask<List<int>, int> {
  final int a;
  final int b;

  _MulTask(this.a, this.b);

  @override
  AsyncTask<List<int>, int> instantiate(
    List<int> parameters, [
    Map<String, SharedData>? sharedData,
  ]) => _MulTask(parameters[0], parameters[1]);

  @override
  List<int> parameters() => [a, b];

  @override
  FutureOr<int> run() => a * b;
}

/// Not present in [_taskTypeRegister].
class _NotRegisteredTask extends AsyncTask<int, int> {
  @override
  AsyncTask<int, int> instantiate(
    int parameters, [
    Map<String, SharedData>? sharedData,
  ]) => _NotRegisteredTask();

  @override
  int parameters() => 0;

  @override
  FutureOr<int> run() => 0;
}

class _UnsendableError {
  // A `ReceivePort` can't be sent to another isolate:
  final RawReceivePort port;

  _UnsendableError(this.port);

  @override
  String toString() => '_UnsendableError';
}

class _UnsendableErrorTask extends AsyncTask<int, int> {
  @override
  AsyncTask<int, int> instantiate(
    int parameters, [
    Map<String, SharedData>? sharedData,
  ]) => _UnsendableErrorTask();

  @override
  int parameters() => 0;

  @override
  FutureOr<int> run() {
    var port = RawReceivePort()..close();
    throw _UnsendableError(port);
  }
}

class _PointerSumTask extends AsyncTask<int, int> {
  final SharedData<SharedPointerUInt32List, SharedPointerAddress> pointer;

  _PointerSumTask(this.pointer);

  @override
  AsyncTask<int, int> instantiate(
    int parameters, [
    Map<String, SharedData>? sharedData,
  ]) => _PointerSumTask(
    sharedData!['pointer']
        as SharedData<SharedPointerUInt32List, SharedPointerAddress>,
  );

  @override
  Map<String, SharedData>? sharedData() => {'pointer': pointer};

  @override
  SharedData? loadSharedData(String key, dynamic serial) {
    var address = serial as SharedPointerAddress;
    return SharedData<SharedPointerUInt32List, SharedPointerAddress>(
      address.getPointer<int>() as SharedPointerUInt32List,
    );
  }

  @override
  int parameters() => 0;

  @override
  FutureOr<int> run() {
    var p = pointer.data;
    var sum = 0;
    for (var i = 0; i < p.length; ++i) {
      sum += p.readAt(i)!;
    }
    return sum;
  }
}
