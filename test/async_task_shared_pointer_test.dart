import 'dart:async';
import 'dart:isolate';
import 'dart:typed_data';

import 'package:async_task/async_task.dart';
import 'package:async_task/src/async_task_shared_pointer.dart';
import 'package:test/test.dart';

void main() {
  group('SharedPointer', () {
    setUp(() {});

    test('SharedPointerUint32', () async {
      var sp = SharedPointerUint32();

      expect(sp.isInitializer, isTrue);
      expect(sp.length, equals(1));
      expect(sp.allocatedBytesLength, equals(4));
      expect(sp.bytesPerValue, equals(4));
      expect(sp.isEmpty, isFalse);

      {
        sp.write(1234);
        expect(sp.read(), equals(1234));

        expect(
            sp.bytes,
            equals(Endian.host == Endian.big
                ? [0, 0, 0x04, 0xD2]
                : [0xD2, 0x04, 0, 0]));

        sp.byteData.setUint8(1, 0x05);
        expect(sp.read(), equals(1490));

        expect(
            sp.bytes,
            equals(Endian.host == Endian.big
                ? [0, 0, 0x05, 0xD2]
                : [0xD2, 0x05, 0, 0]));
      }

      {
        sp.writeAt(0, 12345);
        expect(sp.readAt(0), equals(12345));
        expect(sp.read(), equals(12345));
      }

      for (var i = 0; i < sp.length; ++i) {}

      {
        var serial = sp.serialize();
        expect(serial, isA<SharedPointerAddress>());

        expect(serial.copy(), equals(serial));

        var deserialize = sp.deserialize(serial);
        expect(deserialize, isA<SharedPointerUint32>());

        expect(deserialize.read(), equals(12345));
      }

      var sp2 = sp.address.getPointer() as SharedPointerUint32;

      expect(sp2.isInitializer, isFalse);
      expect(sp2.length, equals(1));
      expect(sp2.allocatedBytesLength, equals(4));
      expect(sp2.isEmpty, isFalse);

      expect(sp2.readAt(0), equals(12345));

      expect(sp.isClosed, isFalse);
      sp.free();
      expect(sp.isClosed, isTrue);
    });

    test('SharedPointerUint64', () async {
      var sp = SharedPointerUint64();

      expect(sp.isInitializer, isTrue);
      expect(sp.length, equals(1));
      expect(sp.allocatedBytesLength, equals(8));
      expect(sp.bytesPerValue, equals(8));
      expect(sp.isEmpty, isFalse);

      {
        sp.write(1234567890123456780);
        expect(sp.read(), equals(1234567890123456780));
      }

      {
        sp.writeAt(0, 1234567890123456785);
        expect(sp.readAt(0), equals(1234567890123456785));
        expect(sp.read(), equals(1234567890123456785));
      }

      for (var i = 0; i < sp.length; ++i) {}

      {
        var serial = sp.serialize();
        expect(serial, isA<SharedPointerAddress>());

        expect(serial.copy(), equals(serial));

        var deserialize = sp.deserialize(serial);
        expect(deserialize, isA<SharedPointerUint64>());

        expect(deserialize.read(), equals(1234567890123456785));
      }

      var sp2 = sp.address.getPointer() as SharedPointerUint64;

      expect(sp2.isInitializer, isFalse);
      expect(sp2.length, equals(1));
      expect(sp2.allocatedBytesLength, equals(8));
      expect(sp2.isEmpty, isFalse);

      expect(sp2.readAt(0), equals(1234567890123456785));

      expect(sp.isClosed, isFalse);
      sp.free();
      expect(sp.isClosed, isTrue);
    });

    test('SharedPointerUint32 + Isolate', () async {
      await _testIsolates(SharedPointerUint32());
    });

    test('SharedPointerUint64 + Isolate', () async {
      await _testIsolates(SharedPointerUint64());
    });

    test('SharedPointerUInt8List', () async {
      var sp = SharedPointerUInt8List(12);

      expect(sp.isInitializer, isTrue);
      expect(sp.length, equals(12));
      expect(sp.allocatedBytesLength, equals(12));
      expect(sp.bytesPerValue, equals(1));
      expect(sp.isEmpty, isFalse);

      for (var i = 0; i < sp.length; ++i) {
        var val = i * 2;
        sp.writeAt(i, val);
        print('byte[$i] >> $val');
      }

      for (var i = 0; i < sp.length; ++i) {
        var val = sp.readAt(i);
        print('byte[$i] << $val');
        expect(val, equals(i * 2));
      }

      expect(sp.view(), equals([0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22]));
      expect(sp.sublist(2, 10), equals([4, 6, 8, 10, 12, 14, 16, 18]));

      {
        var serial = sp.serialize();
        expect(serial, isA<SharedPointerAddress>());

        expect(serial.copy(), equals(serial));

        var deserialize = sp.deserialize(serial);
        expect(deserialize, isA<SharedPointerUInt8List>());

        expect(deserialize.view(),
            equals([0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22]));
      }

      var sp2 = sp.address.getPointer() as SharedPointerUInt8List;

      expect(sp2.isInitializer, isFalse);
      expect(sp2.length, equals(12));
      expect(sp2.allocatedBytesLength, equals(12));
      expect(sp2.isEmpty, isFalse);

      for (var i = 0; i < sp2.length; ++i) {
        var val = sp2.readAt(i);
        print('byte[$i] << $val');
        expect(val, equals(i * 2));
      }

      expect(sp.isClosed, isFalse);
      sp.free();
      expect(sp.isClosed, isTrue);
    });

    test('SharedPointerUInt32List', () async {
      var sp = SharedPointerUInt32List(12);

      expect(sp.isInitializer, isTrue);
      expect(sp.length, equals(12));
      expect(sp.allocatedBytesLength, equals(12 * 4));
      expect(sp.bytesPerValue, equals(4));
      expect(sp.isEmpty, isFalse);

      for (var i = 0; i < sp.length; ++i) {
        var val = 100000 + i;
        sp.writeAt(i, val);
        print('byte[$i] >> $val');
      }

      for (var i = 0; i < sp.length; ++i) {
        var val = sp.readAt(i);
        print('byte[$i] << $val');
        expect(val, equals(100000 + i));
      }

      expect(
          sp.view(),
          equals([
            100000,
            100001,
            100002,
            100003,
            100004,
            100005,
            100006,
            100007,
            100008,
            100009,
            100010,
            100011
          ]));

      expect(
          sp.sublist(2, 10),
          equals([
            100002,
            100003,
            100004,
            100005,
            100006,
            100007,
            100008,
            100009
          ]));

      {
        var serial = sp.serialize();
        expect(serial, isA<SharedPointerAddress>());

        expect(serial.copy(), equals(serial));

        var deserialize = sp.deserialize(serial);
        expect(deserialize, isA<SharedPointerUInt32List>());

        expect(
            deserialize.view(),
            equals([
              100000,
              100001,
              100002,
              100003,
              100004,
              100005,
              100006,
              100007,
              100008,
              100009,
              100010,
              100011
            ]));
      }

      var sp2 = sp.address.getPointer() as SharedPointerUInt32List;

      expect(sp2.isInitializer, isFalse);
      expect(sp2.length, equals(12));
      expect(sp2.allocatedBytesLength, equals(12 * 4));
      expect(sp2.bytesPerValue, equals(4));
      expect(sp2.isEmpty, isFalse);

      for (var i = 0; i < sp2.length; ++i) {
        var val = sp2.readAt(i);
        print('byte[$i] << $val');
        expect(val, equals(100000 + i));
      }

      expect(sp.isClosed, isFalse);
      sp.free();
      expect(sp.isClosed, isTrue);
    });

    test('SharedPointerUInt64List', () async {
      var sp = SharedPointerUInt64List(12);

      expect(sp.isInitializer, isTrue);
      expect(sp.length, equals(12));
      expect(sp.allocatedBytesLength, equals(12 * 8));
      expect(sp.bytesPerValue, equals(8));
      expect(sp.isEmpty, isFalse);

      for (var i = 0; i < sp.length; ++i) {
        var val = 100000 + i;
        sp.writeAt(i, val);
        print('byte[$i] >> $val');
      }

      for (var i = 0; i < sp.length; ++i) {
        var val = sp.readAt(i);
        print('byte[$i] << $val');
        expect(val, equals(100000 + i));
      }

      expect(
          sp.view(),
          equals([
            100000,
            100001,
            100002,
            100003,
            100004,
            100005,
            100006,
            100007,
            100008,
            100009,
            100010,
            100011
          ]));

      expect(
          sp.sublist(2, 10),
          equals([
            100002,
            100003,
            100004,
            100005,
            100006,
            100007,
            100008,
            100009
          ]));

      {
        var serial = sp.serialize();
        expect(serial, isA<SharedPointerAddress>());

        expect(serial.copy(), equals(serial));

        var deserialize = sp.deserialize(serial);
        expect(deserialize, isA<SharedPointerUInt64List>());

        expect(
            deserialize.view(),
            equals([
              100000,
              100001,
              100002,
              100003,
              100004,
              100005,
              100006,
              100007,
              100008,
              100009,
              100010,
              100011
            ]));
      }

      var sp2 = sp.address.getPointer() as SharedPointerUInt64List;

      expect(sp2.isInitializer, isFalse);
      expect(sp2.length, equals(12));
      expect(sp2.allocatedBytesLength, equals(12 * 8));
      expect(sp2.bytesPerValue, equals(8));
      expect(sp2.isEmpty, isFalse);

      for (var i = 0; i < sp2.length; ++i) {
        var val = sp2.readAt(i);
        print('byte[$i] << $val');
        expect(val, equals(100000 + i));
      }

      expect(sp.isClosed, isFalse);
      sp.free();
      expect(sp.isClosed, isTrue);
    });
  });
}

Future<void> _testIsolates(SharedPointer<int> sp) async {
  expect(sp.isClosed, isFalse);
  expect(sp.read(), equals(0));

  var sharedData = SharedData<SharedPointer<int>, SharedPointerAddress>(sp);

  var isolate1Completer = Completer<String>();
  var isolate2Completer = Completer<String>();

  var port1 = RawReceivePort((m) => isolate1Completer.complete(m));
  var port2 = RawReceivePort((m) => isolate2Completer.complete(m));

  Isolate.spawn(isolateSender, [sharedData.serialize(), port1.sendPort]);
  Isolate.spawn(isolateReceiver, [sharedData.serialize(), port2.sendPort]);

  var ret1 = await isolate1Completer.future;
  var ret2 = await isolate2Completer.future;

  expect(ret1, equals('finished>1010'));
  expect(ret2, equals('finished<1010'));

  expect(sp.isClosed, isFalse);

  {
    var serial = sp.serialize();
    expect(serial, isA<SharedPointerAddress>());

    var deserialize = sp.deserialize(serial) as SharedPointer<int>;
    expect(deserialize.runtimeType, equals(sp.runtimeType));

    expect(deserialize.read(), equals(1010));
  }

  sp.free();
  expect(sp.isClosed, isTrue);
}

void isolateSender(List args) async {
  SharedPointerAddress address = args[0];
  SharedPointer<int> sp = address.getPointer();
  SendPort port = args[1];

  print('** Isolate sender started: $sp');

  var lastData = -1;

  for (var i = 0; i <= 10; ++i) {
    var data = 1000 + i;
    sp.write(data);
    print('sent >> $data');
    lastData = data;
    var delay = i <= 1 ? 100 : 10;
    await Future.delayed(Duration(milliseconds: delay));
  }

  port.send('finished>$lastData');

  _closePointerFromIsolate(sp);
}

void isolateReceiver(List args) async {
  SharedPointerAddress address = args[0];
  SharedPointer<int> sp = address.getPointer();
  SendPort port = args[1];

  print('** Isolate reader started: $sp');

  int prevValue = -1;
  while (true) {
    var n = await sp.readNewValue(lastValue: prevValue);
    print('read << $n');
    prevValue = n!;
    if (n >= 1010) break;
  }

  port.send('finished<$prevValue');

  _closePointerFromIsolate(sp);
}

void _closePointerFromIsolate(SharedPointer<int> sp) {
  if (sp.isClosed) throw StateError('Already closed pointer: $sp');
  sp.free();
  if (!sp.isClosed) throw StateError('Close error: $sp');
}
