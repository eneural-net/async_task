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
    });

    test('List<double>', () {
      var ns = <double>[1, 2, 3, 4, 5, 6, 7, 8];
      var serial = SerializableData.serializeGeneric(ns);

      var ns2 = SerializableData.deserializeGeneric<List<double>>(serial);

      expect(ns2.length, equals(8));
      expect(ns2, equals(<double>[1, 2, 3, 4, 5, 6, 7, 8]));
    });

    test('List<String>', () {
      var s = <String>['a', 'b', 'c'];
      var serial = SerializableData.serializeGeneric(s);

      var s2 = SerializableData.deserializeGeneric<List<String>>(serial);

      expect(s2.length, equals(3));
      expect(s2, equals(<String>['a', 'b', 'c']));
    });
  });
}
