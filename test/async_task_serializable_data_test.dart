import 'package:async_task/async_task.dart';
import 'package:test/test.dart';

void main() {
  group('SerializableData.serializeGeneric', () {
    setUp(() {});

    test('List<int>', () {
      var ns = <int>[1, 2, 3, 4, 5, 6, 7, 8];
      var serial = SerializableData.serializeGeneric(ns);

      var ns2 = SerializableData.deserializeGeneric<List<int>>(serial);

      expect(ns2.length, equals(8));
      expect(ns2, equals(<int>[1, 2, 3, 4, 5, 6, 7, 8]));

      expect(SerializableData.computeGenericSignature(ns),
          matches(RegExp(r'list>\d+')));
    });

    test('List<double>', () {
      var ns = <double>[1, 2, 3, 4, 5, 6, 7, 8];
      var serial = SerializableData.serializeGeneric(ns);

      var ns2 = SerializableData.deserializeGeneric<List<double>>(serial);

      expect(ns2.length, equals(8));
      expect(ns2, equals(<double>[1, 2, 3, 4, 5, 6, 7, 8]));

      expect(SerializableData.computeGenericSignature(ns),
          matches(RegExp(r'list>\d+')));
    });

    test('List<String>', () {
      var s = <String>['a', 'b', 'c'];
      var serial = SerializableData.serializeGeneric(s);

      var s2 = SerializableData.deserializeGeneric<List<String>>(serial);

      expect(s2.length, equals(3));
      expect(s2, equals(<String>['a', 'b', 'c']));

      expect(SerializableData.computeGenericSignature(s),
          matches(RegExp(r'list>\d+')));
    });

    test('computeGenericSignature', () {
      expect(SerializableData.computeGenericSignature(10),
          matches(RegExp(r'int>\d+')));
      expect(SerializableData.computeGenericSignature(10.0),
          matches(RegExp(r'(?:double|int)>\d+')));
      expect(SerializableData.computeGenericSignature(10.1),
          matches(RegExp(r'double>\d+')));
      expect(SerializableData.computeGenericSignature(true),
          matches(RegExp(r'bool>true')));
      expect(SerializableData.computeGenericSignature('abc'),
          matches(RegExp(r'str>\d+')));
      expect(SerializableData.computeGenericSignature([1, 2]),
          matches(RegExp(r'list>\d+')));
      expect(SerializableData.computeGenericSignature({'a': 1}),
          matches(RegExp(r'map>\d+')));
      expect(SerializableData.computeGenericSignature({'a', 'b'}),
          matches(RegExp(r'set>\d+')));
    });

    test('copyGeneric', () {
      void cp<T>(T o) {
        var o2 = SerializableData.copyGeneric<T>(o);
        expect(o2, equals(o));
        expect('$o2', equals('$o'));
      }

      cp(10);
      cp(10.0);
      cp(true);
      cp(false);
      cp<dynamic>(<int>[1, 2, 3]);
      cp<dynamic>(<String, int>{'a': 10});
      cp<dynamic>(<String>{'a', 'b'});
    });
  });
}
