import 'dart:typed_data';

import 'package:async_task/async_task.dart';
import 'package:async_task/async_task_extension.dart';
import 'package:test/test.dart';

void main() {
  final nativePlatform = AsyncTaskPlatform(AsyncTaskPlatformType.isolate, 2);
  final genericPlatform = AsyncTaskPlatform(AsyncTaskPlatformType.generic, 1);

  group('AsyncTaskPlatform', () {
    test('isNative / isGeneric', () {
      expect(nativePlatform.isNative, isTrue);
      expect(nativePlatform.isGeneric, isFalse);
      expect(genericPlatform.isNative, isFalse);
      expect(genericPlatform.isGeneric, isTrue);
      expect(nativePlatform.maximumParallelism, equals(2));
      expect('$nativePlatform', contains('maximumParallelism: 2'));
    });
  });

  group('SharedData', () {
    test('signature, serializeCached and reset', () {
      var computations = 0;
      var sd = SharedData<int, int>(
        123,
        signatureComputer: (d) => 'sig:${++computations}:$d',
      );

      expect(sd.signature, equals('sig:1:123'));
      // Computed only once:
      expect(sd.signature, equals('sig:1:123'));
      expect(computations, equals(1));

      expect(sd.serializeCached(), equals(123));

      sd.reset();
      expect(sd.signature, equals('sig:2:123'));
    });

    test('default signature', () {
      expect(SharedData<int, int>(10).signature, equals('int>10'));
      expect(
        SharedData<List<int>, List<int>>([1, 2]).signature,
        equals(SharedData<List<int>, List<int>>([1, 2]).signature),
      );
    });

    test('custom serializer / deserializer', () {
      var sd = SharedData<int, String>(
        42,
        serializer: (d) => 'n=$d',
        deserializer: (s) => SharedData<int, String>(int.parse(s.substring(2))),
      );

      var serial = sd.serialize();
      expect(serial, equals('n=42'));

      var sd2 = sd.deserialize(serial);
      expect(sd2.data, equals(42));
    });

    test('generic serialize / deserialize', () {
      var sd = SharedData<List<int>, List<int>>([1, 2, 3]);
      var serial = sd.serialize(platform: genericPlatform);
      expect(serial, equals([1, 2, 3]));

      var sd2 = sd.deserialize(serial, platform: genericPlatform);
      expect(sd2.data, equals([1, 2, 3]));
    });

    test('noSerializationOnDartNative', () {
      var data = [1, 2, 3];
      var sd = SharedData<List<int>, List<int>>(
        data,
        noSerializationOnDartNative: true,
      );

      // Native platform: the object is passed as is.
      expect(identical(sd.serialize(platform: nativePlatform), data), isTrue);
      var sd2 = sd.deserialize(data, platform: nativePlatform);
      expect(identical(sd2.data, data), isTrue);
      expect(sd2.noSerializationOnDartNative, isTrue);

      // Generic platform: normal serialization.
      expect(sd.serialize(platform: genericPlatform), equals(data));
    });

    test('noSerializationOnDartNative + requiresSerializationToBeShared', () {
      var sd = SharedData<_Point, List<int>>(
        _Point(1, 2, requiresSerialization: true),
        noSerializationOnDartNative: true,
      );

      expect(sd.serialize(platform: nativePlatform), equals([1, 2]));

      var sd2 = SharedData<_Point, _Point>(
        _Point(3, 4),
        noSerializationOnDartNative: true,
      );
      var serial = sd2.serialize(platform: nativePlatform);
      expect(identical(serial, sd2.data), isTrue);
      var sd3 = sd2.deserialize(serial, platform: nativePlatform);
      expect(identical(sd3.data, sd2.data), isTrue);
    });

    test('cast', () {
      var sd = SharedData<int, int>(5);
      expect(identical(sd.cast<num, num>(), sd), isTrue);

      var sdObj = SharedData<Object, Object>(7);
      var sdInt = sdObj.cast<int, int>();
      expect(sdInt, isA<SharedData<int, int>>());
      expect(sdInt.data, equals(7));
    });

    test('copy', () {
      var sd = SharedData<int, int>(1);
      expect(() => sd.copy(), throwsStateError);

      sd.allowCopyBySerialization = true;
      expect(sd.copy().data, equals(1));

      var p = _Point(1, 2);
      var sdPoint = SharedData<_Point, List<int>>(p);
      var sdPoint2 = sdPoint.copy();
      expect(identical(sdPoint2.data, p), isFalse);
      expect(sdPoint2.data, equals(p));
    });
  });

  group('AsyncExecutorSharedDataInfo', () {
    test('isEmpty / toString', () {
      var info = AsyncExecutorSharedDataInfo();
      expect(info.isEmpty, isTrue);
      expect('$info', equals('AsyncExecutorSharedDataInfo{ empty }'));

      info.sentSharedDataSignatures.add('a');
      expect(info.isEmpty, isFalse);
      expect('$info', contains('sent: {a}'));
      expect('$info', contains('disposed: {}'));
    });
  });

  group('SerializableData', () {
    test('SerializableData default deserialize', () {
      var p = _Point(5, 6);
      var p2 = p.deserialize(p.serialize());
      expect(p2, equals(p));
      expect(p.requiresSerializationToBeShared, isFalse);
    });

    test('computeGenericSignature', () {
      expect(
        SerializableData.computeGenericSignature(_Point(1, 2)),
        equals('point>1,2'),
      );
      expect(
        SerializableData.computeGenericSignature(Float32x4(1, 2, 3, 4)),
        startsWith('f32x4>'),
      );
      expect(
        SerializableData.computeGenericSignature(Int32x4(1, 2, 3, 4)),
        startsWith('i32x4>'),
      );
      expect(
        SerializableData.computeGenericSignature([1, 2].map((e) => e)),
        startsWith('iter>'),
      );
      expect(
        SerializableData.computeGenericSignature(Object()),
        startsWith('obj:'),
      );

      // Same content => same signature:
      expect(
        SerializableData.computeGenericSignature({'a': 1, 'b': 2}),
        equals(SerializableData.computeGenericSignature({'a': 1, 'b': 2})),
      );
    });

    test('copyGeneric: basic values and SIMD', () {
      expect(SerializableData.copyGeneric('abc'), equals('abc'));
      expect(SerializableData.copyGeneric(1.5), equals(1.5));
      expect(SerializableData.copyGeneric<num>(2), equals(2));

      var f = Float32x4(1, 2, 3, 4);
      expect(identical(SerializableData.copyGeneric(f), f), isTrue);
      var i = Int32x4(1, 2, 3, 4);
      expect(identical(SerializableData.copyGeneric(i), i), isTrue);

      var p = _Point(1, 2);
      var p2 = SerializableData.copyGeneric(p);
      expect(identical(p2, p), isFalse);
      expect(p2, equals(p));
    });

    test('copyGeneric: MapEntry', () {
      // The copied entry has dynamic types (generic copy).
      MapEntry e = SerializableData.copyGeneric<dynamic>(MapEntry('k', 10));
      expect(e.key, equals('k'));
      expect(e.value, equals(10));
    });

    test('copyGeneric: typed lists', () {
      void check<T extends List>(T list) {
        var copy = SerializableData.copyGeneric<T>(list);
        expect(copy, isA<T>());
        expect(identical(copy, list), isFalse);
        expect(copy, equals(list));
      }

      check(Uint8List.fromList([1, 2, 3]));
      check(Uint16List.fromList([1, 2, 3]));
      check(Uint32List.fromList([1, 2, 3]));
      check(Int8List.fromList([-1, 2, 3]));
      check(Int16List.fromList([-1, 2, 3]));
      check(Int32List.fromList([-1, 2, 3]));
      check(Float32List.fromList([1.5, 2.5]));
      check(Float64List.fromList([1.5, 2.5]));

      var f32x4 = Float32x4List.fromList([Float32x4(1, 2, 3, 4)]);
      var f32x4Copy = SerializableData.copyGeneric(f32x4);
      expect(identical(f32x4Copy, f32x4), isFalse);
      expect(f32x4Copy.toDoubles(), equals([1, 2, 3, 4]));

      var i32x4 = Int32x4List.fromList([Int32x4(1, 2, 3, 4)]);
      var i32x4Copy = SerializableData.copyGeneric(i32x4);
      expect(identical(i32x4Copy, i32x4), isFalse);
      expect(i32x4Copy.toInts(), equals([1, 2, 3, 4]));
    });

    test('copyGeneric: collections are deep copied', () {
      // Generic collection copies are untyped (`dynamic` elements).
      var list = <Object>[
        1,
        [2, 3],
      ];
      List listCopy = SerializableData.copyGeneric<dynamic>(list);
      expect(listCopy, equals(list));
      expect(identical(listCopy[1], list[1]), isFalse);

      var set = <int>{1, 2};
      Set setCopy = SerializableData.copyGeneric<dynamic>(set);
      expect(setCopy, equals(set));
      expect(identical(setCopy, set), isFalse);

      var iter = [1, 2].map((e) => e * 10);
      expect(SerializableData.copyGeneric<dynamic>(iter), equals([10, 20]));
    });

    test('copyGeneric: unsupported type', () {
      expect(() => SerializableData.copyGeneric(Object()), throwsStateError);
    });

    test('serializeGeneric / deserializeGeneric: basic values', () {
      expect(SerializableData.serializeGeneric(10), equals(10));
      expect(SerializableData.serializeGeneric(1.5), equals(1.5));
      expect(SerializableData.serializeGeneric('a'), equals('a'));
      expect(SerializableData.serializeGeneric(true), equals(true));
      expect(SerializableData.serializeGeneric(_Point(1, 2)), equals([1, 2]));

      expect(SerializableData.deserializeGeneric<int>(10), equals(10));
      expect(SerializableData.deserializeGeneric<double>(1.5), equals(1.5));
      expect(SerializableData.deserializeGeneric<num>(3), equals(3));
      expect(SerializableData.deserializeGeneric<String>('a'), equals('a'));
      expect(SerializableData.deserializeGeneric<bool>(false), isFalse);

      var f = Float32x4(1, 2, 3, 4);
      expect(
        identical(SerializableData.serializeGeneric<Float32x4>(f), f),
        isTrue,
      );
      expect(
        identical(SerializableData.deserializeGeneric<Float32x4>(f), f),
        isTrue,
      );
      var i = Int32x4(1, 2, 3, 4);
      expect(
        identical(SerializableData.serializeGeneric<Int32x4>(i), i),
        isTrue,
      );
      expect(
        identical(SerializableData.deserializeGeneric<Int32x4>(i), i),
        isTrue,
      );
    });

    test('serializeGeneric: MapEntry', () {
      MapEntry e = SerializableData.serializeGeneric(MapEntry('k', 10));
      expect(e.key, equals('k'));
      expect(e.value, equals(10));

      var e2 = SerializableData.deserializeGeneric<MapEntry>(e);
      expect(e2.key, equals('k'));
      expect(e2.value, equals(10));
    });

    test('serializeGeneric / deserializeGeneric: typed lists', () {
      void check<T extends List>(T list) {
        var serial = SerializableData.serializeGeneric(list);
        var list2 = SerializableData.deserializeGeneric<T>(serial);
        expect(list2, isA<T>());
        expect(list2, equals(list));
      }

      check(Uint8List.fromList([1, 2, 3]));
      check(Uint16List.fromList([1, 2, 3]));
      check(Uint32List.fromList([1, 2, 3]));
      check(Int8List.fromList([-1, 2, 3]));
      check(Int16List.fromList([-1, 2, 3]));
      check(Int32List.fromList([-1, 2, 3]));
      check(Float32List.fromList([1.5, 2.5]));
      check(Float64List.fromList([1.5, 2.5]));

      var f32x4 = Float32x4List.fromList([
        Float32x4(1, 2, 3, 4),
        Float32x4(5, 6, 7, 8),
      ]);
      var f32x4Serial = SerializableData.serializeGeneric(f32x4);
      expect(f32x4Serial, equals([1, 2, 3, 4, 5, 6, 7, 8]));
      var f32x4b = SerializableData.deserializeGeneric<Float32x4List>(
        f32x4Serial,
      );
      expect(f32x4b.toDoubles(), equals([1, 2, 3, 4, 5, 6, 7, 8]));

      var i32x4 = Int32x4List.fromList([Int32x4(1, 2, 3, 4)]);
      var i32x4Serial = SerializableData.serializeGeneric(i32x4);
      expect(i32x4Serial, equals([1, 2, 3, 4]));
      var i32x4b = SerializableData.deserializeGeneric<Int32x4List>(
        i32x4Serial,
      );
      expect(i32x4b.toInts(), equals([1, 2, 3, 4]));
    });

    test('serializeGeneric / deserializeGeneric: collections', () {
      // Basic collections are passed as is:
      var ints = <int>[1, 2];
      expect(identical(SerializableData.serializeGeneric(ints), ints), isTrue);
      expect(
        SerializableData.deserializeGeneric<Set<int>>(<int>{1, 2}),
        equals({1, 2}),
      );
      expect(
        SerializableData.deserializeGeneric<Iterable<String>>(['a']),
        equals(['a']),
      );

      // Non basic collections are serialized element by element:
      var points = <Object>[_Point(1, 2), 3];
      expect(
        SerializableData.serializeGeneric(points),
        equals([
          [1, 2],
          3,
        ]),
      );

      var pointsSet = <Object>{_Point(1, 2)};
      expect(
        SerializableData.serializeGeneric(pointsSet),
        equals({
          [1, 2],
        }),
      );

      var map = <String, Object>{'p': _Point(1, 2)};
      expect(
        SerializableData.serializeGeneric(map),
        equals({
          'p': [1, 2],
        }),
      );

      var iter = <Object>[_Point(3, 4)].map((e) => e);
      expect(
        SerializableData.serializeGeneric(iter),
        equals([
          [3, 4],
        ]),
      );

      // Dynamic collections are deserialized element by element:
      expect(
        SerializableData.deserializeGeneric<List>([1, 'a', true]),
        equals([1, 'a', true]),
      );
      expect(
        SerializableData.deserializeGeneric<Set>({1, 'a'}),
        equals({1, 'a'}),
      );
      expect(
        SerializableData.deserializeGeneric<Iterable>([1, 2]),
        equals([1, 2]),
      );
      expect(
        SerializableData.deserializeGeneric<Map>({'a': 1}),
        equals({'a': 1}),
      );
    });

    test('serializeGeneric / deserializeGeneric: unsupported types', () {
      expect(
        () => SerializableData.serializeGeneric(Object()),
        throwsStateError,
      );
      expect(
        () => SerializableData.deserializeGeneric<_Point>([1, 2]),
        throwsStateError,
      );
    });
  });

  group('collection extensions', () {
    test('ListExtension', () {
      var l = <int>[1, 2, 3];
      expect(l.genericType, equals(int));

      var c = l.copy();
      expect(identical(c, l), isFalse);
      expect(c.deepEquals(l), isTrue);
      expect(c.computeHashcode(), equals(l.computeHashcode()));
      expect(l.deepCopy(), equals(l));
    });

    test('MapExtension', () {
      var m = <String, int>{'a': 1, 'b': 2};
      expect(m.keyType, equals(String));
      expect(m.valueType, equals(int));

      var c = m.copy();
      expect(identical(c, m), isFalse);
      expect(c.deepEquals(m), isTrue);
      expect(c.computeHashcode(), equals(m.computeHashcode()));
      expect(m.deepCopy(), equals(m));

      // Basic key/value types: serialized as is.
      expect(identical(m.serialize(), m), isTrue);

      c.removeAllKeys(['a', 'x']);
      expect(c, equals({'b': 2}));
    });

    test('SetExtension', () {
      var s = <int>{1, 2};
      var c = s.copy();
      expect(identical(c, s), isFalse);
      expect(c.deepEquals(s), isTrue);
      expect(c.computeHashcode(), equals(s.computeHashcode()));
      expect(s.deepCopy(), equals(s));
      expect(identical(s.serialize(), s), isTrue);

      for (var basic in <Set>[
        <double>{1.5},
        <num>{1},
        <bool>{true},
        <String>{'a'},
      ]) {
        expect(identical(basic.serialize(), basic), isTrue);
      }
    });

    test('IterableExtension', () {
      var it = [1, 2].map((e) => e);
      expect(it.copy(), equals([1, 2]));
      expect(it.deepCopy(), equals([1, 2]));
      expect(it.deepEquals({1, 2}), isTrue);
      expect(
        it.computeHashcode(),
        equals([1, 2].map((e) => e).computeHashcode()),
      );
      expect(it.serialize(), equals([1, 2]));

      for (var basic in <Iterable>[
        [1.5].map<double>((e) => e),
        <num>[1].map<num>((e) => e),
        [true].map<bool>((e) => e),
        ['a'].map<String>((e) => e),
      ]) {
        expect(identical(basic.serialize(), basic), isTrue);
      }
    });

    test('List basic types serialization', () {
      for (var basic in <List>[
        <double>[1.5],
        <num>[1],
        <bool>[true],
        <String>['a'],
      ]) {
        expect(identical(basic.serialize(), basic), isTrue);
      }
    });

    test('ListDoubleExtension.toFloat64x2List', () {
      var ds = <double>[1, 2, 3, 4];
      var f64x2 = ds.toFloat64x2List();

      expect(f64x2.length, equals(2));
      expect(f64x2.toDoubles(), equals([1, 2, 3, 4]));
      expect(f64x2.toInts(), equals([1, 2, 3, 4]));
      expect(f64x2.serialize(), equals([1, 2, 3, 4]));

      expect(
        SerializableData.deserializeGeneric<Float64x2List>(
          SerializableData.serializeGeneric(f64x2),
        ).toDoubles(),
        equals([1, 2, 3, 4]),
      );
    });

    test('SIMD List (not typed) extensions', () {
      var f32x4 = <Float32x4>[Float32x4(1, 2, 3, 4), Float32x4(5, 6, 7, 8)];
      expect(f32x4.toDoubles(), equals([1, 2, 3, 4, 5, 6, 7, 8]));
      expect(f32x4.toInts(), equals([1, 2, 3, 4, 5, 6, 7, 8]));
      expect(f32x4.serialize(), equals([1, 2, 3, 4, 5, 6, 7, 8]));

      var i32x4 = <Int32x4>[Int32x4(1, 2, 3, 4), Int32x4(5, 6, 7, 8)];
      expect(i32x4.toInts(), equals([1, 2, 3, 4, 5, 6, 7, 8]));
      expect(i32x4.toDoubles(), equals([1, 2, 3, 4, 5, 6, 7, 8]));
      expect(i32x4.serialize(), equals([1, 2, 3, 4, 5, 6, 7, 8]));

      var f64x2 = <Float64x2>[Float64x2(1, 2), Float64x2(3, 4)];
      expect(f64x2.toDoubles(), equals([1, 2, 3, 4]));
      expect(f64x2.toInts(), equals([1, 2, 3, 4]));
      expect(f64x2.serialize(), equals([1, 2, 3, 4]));

      var f32x4List = Float32x4List.fromList(f32x4);
      expect(f32x4List.toInts(), equals([1, 2, 3, 4, 5, 6, 7, 8]));

      var i32x4List = Int32x4List.fromList(i32x4);
      expect(i32x4List.toDoubles(), equals([1, 2, 3, 4, 5, 6, 7, 8]));
    });

    test('suppressUnhandledError', () async {
      var f = Future<int>.error(StateError('suppressed'));
      f.suppressUnhandledError();
      // The error is still delivered to explicit listeners:
      await expectLater(f, throwsStateError);
    });
  });
}

class _Point extends SerializableData<_Point, List<int>> {
  final int x;
  final int y;

  final bool requiresSerialization;

  _Point(this.x, this.y, {this.requiresSerialization = false});

  @override
  bool get requiresSerializationToBeShared => requiresSerialization;

  @override
  _Point instantiate(_Point data) => data;

  @override
  _Point copy() => _Point(x, y);

  @override
  _Point deserializeData(List<int> serial) => _Point(serial[0], serial[1]);

  @override
  List<int> serialize() => [x, y];

  @override
  String computeSignature() => 'point>$x,$y';

  @override
  bool operator ==(Object other) =>
      other is _Point && other.x == x && other.y == y;

  @override
  int get hashCode => Object.hash(x, y);

  @override
  String toString() => '_Point($x, $y)';
}
