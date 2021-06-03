import 'package:async_task/async_task_extension.dart';
import 'package:test/test.dart';

void main() {
  group('extension tests', () {
    setUp(() {});

    test('ListIntExtension', () {
      var ns = <int>[1, 2, 3, 4, 5, 6, 7, 8];
      var ns2 = ns.toInt32x4List();

      expect(ns2.length, equals(2));
      expect(ns2.toInts(), equals(<int>[1, 2, 3, 4, 5, 6, 7, 8]));
      expect(ns2.toDoubles(), equals(<double>[1, 2, 3, 4, 5, 6, 7, 8]));

      var ds = ns.toFloat32x4List();

      expect(ds.length, equals(2));
      expect(ds.toInts(), equals(<int>[1, 2, 3, 4, 5, 6, 7, 8]));
      expect(ds.toDoubles(), equals(<double>[1, 2, 3, 4, 5, 6, 7, 8]));
    });

    test('ListDoubleExtension', () {
      var ns = <double>[1, 2, 3, 4, 5, 6, 7, 8];
      var ds = ns.toFloat32x4List();

      expect(ds.length, equals(2));
      expect(ds.toInts(), equals(<int>[1, 2, 3, 4, 5, 6, 7, 8]));
      expect(ds.toDoubles(), equals(<double>[1, 2, 3, 4, 5, 6, 7, 8]));

      var ns2 = ns.toInt32x4List();

      expect(ns2.length, equals(2));
      expect(ns2.toInts(), equals(<int>[1, 2, 3, 4, 5, 6, 7, 8]));
      expect(ns2.toDoubles(), equals(<double>[1, 2, 3, 4, 5, 6, 7, 8]));
    });
  });
}
