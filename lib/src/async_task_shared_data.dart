import 'dart:typed_data';

import 'package:async_task/async_task.dart';

import 'async_task_extension.dart';

/// Interface for serializable data classes:
abstract class SerializableData<D, S> {
  /// Forces serialization and deserialization even when [SharedData.noSerializationOnDartNative] is `true`.
  bool get requiresSerializationToBeShared => false;

  /// Creates a new instance with [data].
  SerializableData<D, S> instantiate(D data);

  /// Creates a copy of this instance.
  SerializableData<D, S> copy();

  /// Deserializes [serial] to [D].
  D deserializeData(S serial);

  /// Deserializes [serial] to a instance of this type.
  SerializableData<D, S> deserialize(S serial) =>
      instantiate(deserializeData(serial));

  /// Serializes this instance to [S].
  S serialize();

  /// Computes the signature of this instance.
  String computeSignature();

  /// Computes a generic signature for [SerializableData] and basic Dart types:
  /// primitive values (null, num, bool, double, String),
  /// lists and maps whose elements are any of these.
  static String computeGenericSignature<D>(D data) {
    if (data is SerializableData) {
      return data.computeSignature();
    } else if (data is int) {
      return 'int>$data';
    } else if (data is double) {
      return 'double>$data';
    } else if (data is num) {
      return 'num>$data';
    } else if (data is String) {
      var h = data.hashCode;
      return 'str>$h';
    } else if (data is bool) {
      return 'bool>$data';
    } else if (data is Float32x4) {
      return 'f32x4>$data';
    } else if (data is Int32x4) {
      return 'i32x4>$data';
    } else if (data is Float64x2) {
      return 'i64x2>$data';
    } else if (data is List) {
      var h = data.computeHashcode();
      return 'list>$h';
    } else if (data is Map) {
      var h = data.computeHashcode();
      return 'map>$h';
    } else if (data is Set) {
      var h = data.computeHashcode();
      return 'set>$h';
    } else if (data is Iterable) {
      var h = data.computeHashcode();
      return 'iter>$h';
    } else {
      var h = data.hashCode;
      return 'obj:$h';
    }
  }

  /// Creates generic copy for [SerializableData] and basic Dart types:
  /// primitive values (null, num, bool, double, String),
  /// lists and maps whose elements are any of these.
  static D copyGeneric<D>(D data) {
    if (data is SerializableData) {
      return data.copy() as D;
    } else if (data is int) {
      return data;
    } else if (data is double) {
      return data;
    } else if (data is num) {
      return data;
    } else if (data is String) {
      return data;
    } else if (data is bool) {
      return data;
    } else if (data is Float32x4) {
      return data;
    } else if (data is Int32x4) {
      return data;
    } else if (data is Float64x2) {
      return data;
    } else if (data is MapEntry) {
      return MapEntry(copyGeneric(data.key), copyGeneric(data.key)) as D;
    } else if (data is Uint8List) {
      return Uint8List.fromList(data) as D;
    } else if (data is Uint16List) {
      return Uint16List.fromList(data) as D;
    } else if (data is Uint32List) {
      return Uint32List.fromList(data) as D;
    } else if (data is Int8List) {
      return Int8List.fromList(data) as D;
    } else if (data is Int16List) {
      return Int8List.fromList(data) as D;
    } else if (data is Int32List) {
      return Int32List.fromList(data) as D;
    } else if (data is Float32List) {
      return Float32List.fromList(data) as D;
    } else if (data is Float64List) {
      return Float32List.fromList(data) as D;
    } else if (data is Float32x4List) {
      return Float32x4List.fromList(data) as D;
    } else if (data is Int32x4List) {
      return Int32x4List.fromList(data) as D;
    } else if (data is Float64x2List) {
      return Float64x2List.fromList(data) as D;
    } else if (data is List) {
      return data.deepCopy() as D;
    } else if (data is Map) {
      return data.deepCopy() as D;
    } else if (data is Set) {
      return data.deepCopy() as D;
    } else if (data is Iterable) {
      return data.deepCopy() as D;
    } else {
      throw StateError(
          "Can't perform a generic copy on type: ${data.runtimeType}");
    }
  }

  /// Generic serialization of [SerializableData] and basic Dart types:
  /// primitive values (null, num, bool, double, String),
  /// lists and maps whose elements are any of these.
  static S serializeGeneric<S>(dynamic data) {
    if (data is SerializableData) {
      return data.serialize() as S;
    } else if (data is int) {
      return data as S;
    } else if (data is double) {
      return data as S;
    } else if (data is num) {
      return data as S;
    } else if (data is String) {
      return data as S;
    } else if (data is bool) {
      return data as S;
    } else if (data is Float32x4) {
      return data as S;
    } else if (data is Int32x4) {
      return data as S;
    } else if (data is Float64x2) {
      return data as S;
    } else if (data is MapEntry) {
      return MapEntry(serializeGeneric(data.key), serializeGeneric(data.key))
          as S;
    } else if (data is Uint8List) {
      return data.serialize() as S;
    } else if (data is Uint16List) {
      return data.serialize() as S;
    } else if (data is Uint32List) {
      return data.serialize() as S;
    } else if (data is Int8List) {
      return data.serialize() as S;
    } else if (data is Int16List) {
      return data.serialize() as S;
    } else if (data is Int32List) {
      return data.serialize() as S;
    } else if (data is Float32List) {
      return data.serialize() as S;
    } else if (data is Float64List) {
      return data.serialize() as S;
    } else if (data is Float32x4List) {
      return data.serialize() as S;
    } else if (data is Int32x4List) {
      return data.serialize() as S;
    } else if (data is Float64x2List) {
      return data.serialize() as S;
    } else if (data is List) {
      return data.serialize() as S;
    } else if (data is Map) {
      return data.serialize() as S;
    } else if (data is Set) {
      return data.serialize() as S;
    } else if (data is Iterable) {
      return data.serialize() as S;
    } else {
      throw StateError(
          "Can't perform a generic serialization on type: ${data.runtimeType}");
    }
  }

  static Type _toType<T>() => T;

  static final _typeListInt = _toType<List<int>>();
  static final _typeListDouble = _toType<List<double>>();
  static final _typeListNum = _toType<List<num>>();
  static final _typeListBool = _toType<List<bool>>();
  static final _typeListString = _toType<List<String>>();

  static final _typeSetInt = _toType<Set<int>>();
  static final _typeSetDouble = _toType<Set<double>>();
  static final _typeSetNum = _toType<Set<num>>();
  static final _typeSetBool = _toType<Set<bool>>();
  static final _typeSetString = _toType<Set<String>>();

  static final _typeIterableInt = _toType<Iterable<int>>();
  static final _typeIterableDouble = _toType<Iterable<double>>();
  static final _typeIterableNum = _toType<Iterable<num>>();
  static final _typeIterableBool = _toType<Iterable<bool>>();
  static final _typeIterableString = _toType<Iterable<String>>();

  static bool _isBasicCollectionType(Type t) {
    return (t == _typeListInt ||
        t == _typeListDouble ||
        t == _typeListNum ||
        t == _typeListBool ||
        t == _typeListString ||
        t == _typeSetInt ||
        t == _typeSetDouble ||
        t == _typeSetNum ||
        t == _typeSetBool ||
        t == _typeSetString ||
        t == _typeIterableInt ||
        t == _typeIterableDouble ||
        t == _typeIterableNum ||
        t == _typeIterableBool ||
        t == _typeIterableString);
  }

  static D deserializeGeneric<D>(dynamic serial) {
    if (D == int) {
      return serial as D;
    } else if (D == double) {
      return serial as D;
    } else if (D == num) {
      return serial as D;
    } else if (D == String) {
      return serial as D;
    } else if (D == bool) {
      return serial as D;
    } else if (D == Float32x4) {
      return serial as D;
    } else if (D == Int32x4) {
      return serial as D;
    } else if (D == Float64x2) {
      return serial as D;
    } else if (D == MapEntry) {
      var e = serial as MapEntry;
      return MapEntry(deserializeGeneric(e.key), deserializeGeneric(e.key))
          as D;
    } else if (D == Uint8List) {
      return Uint8List.fromList(serial as List<int>) as D;
    } else if (D == Uint16List) {
      return Uint16List.fromList(serial as List<int>) as D;
    } else if (D == Uint32List) {
      return Uint32List.fromList(serial as List<int>) as D;
    } else if (D == Int8List) {
      return Int8List.fromList(serial as List<int>) as D;
    } else if (D == Int16List) {
      return Int16List.fromList(serial as List<int>) as D;
    } else if (D == Int32List) {
      return Int32List.fromList(serial as List<int>) as D;
    } else if (D == Float32List) {
      return Float32List.fromList(serial as List<double>) as D;
    } else if (D == Float64List) {
      return Float64List.fromList(serial as List<double>) as D;
    } else if (D == Float32x4List) {
      var e = serial as List<double>;
      return e.toFloat32x4List() as D;
    } else if (D == Int32x4List) {
      var e = serial as List<int>;
      return e.toInt32x4List() as D;
    } else if (D == Float64x2List) {
      var e = serial as List<double>;
      return e.toFloat64x2List() as D;
    } else if (_isBasicCollectionType(D)) {
      return serial as D;
    } else if (D == List) {
      var e = serial as List;
      return e.map(deserializeGeneric).toList() as D;
    } else if (D == Map) {
      var e = serial as Map;
      return e as D;
    } else if (D == Set) {
      var e = serial as Set;
      return e.map(deserializeGeneric).toSet() as D;
    } else if (D == Iterable) {
      var e = serial as Iterable;
      return e.map(deserializeGeneric).toList() as D;
    } else {
      throw StateError("Can't perform a generic deserialization on type: $D");
    }
  }
}

/// Class for shared data between [AsyncTask] instances.
///
/// This instance will be transferred to the executor thread/isolate only
/// once, and not per task, avoiding the [Isolate] messaging bottle neck.
///
/// - The internal [data] should NOT be modified after instantiated.
/// - [S] should be a simple type that can be sent through threads/isolates or
///   serialized as JSON.
/// - The default implementation supports Dart basic types:
///   primitive values (null, num, bool, double, String),
///   lists and maps whose elements are any of these.
class SharedData<D, S> {
  /// The actual data.
  final D data;

  /// If true no serialization will be performed when transferring
  /// this data to another [Isolate]. A platform dependent object memory
  /// copy will be performed.
  bool noSerializationOnDartNative;

  SharedData(this.data,
      {this.serializer,
      this.deserializer,
      this.signatureComputer,
      this.noSerializationOnDartNative = false});

  /// Casts to [SharedData<X, Y>].
  SharedData<X, Y> cast<X, Y>() {
    if (this is SharedData<X, Y>) {
      return this as SharedData<X, Y>;
    } else {
      return SharedData<X, Y>(data as X);
    }
  }

  bool allowCopyBySerialization = false;

  /// Creates a copy of this instance.
  SharedData<D, S> copy() {
    var data = this.data;
    if (data is SerializableData) {
      return SharedData<D, S>(data.copy() as D);
    } else if (allowCopyBySerialization) {
      return deserialize(serialize());
    } else {
      throw StateError("Can't perform a copy");
    }
  }

  SharedData<D, S> Function(S serial)? deserializer;

  /// Deserialize a [SharedData] from [serial].
  SharedData<D, S> deserialize(S serial, {AsyncTaskPlatform? platform}) {
    if (noSerializationOnDartNative && (platform?.isNative ?? false)) {
      D data;
      if (serial is SerializableData &&
          serial.requiresSerializationToBeShared) {
        data = serial.deserializeData(serial) as D;
      } else {
        data = serial as D;
      }
      return SharedData<D, S>(data,
          noSerializationOnDartNative: noSerializationOnDartNative);
    } else if (deserializer != null) {
      return deserializer!(serial);
    } else {
      var data = SerializableData.deserializeGeneric<D>(serial);
      return SharedData(data);
    }
  }

  S Function(D data)? serializer;

  /// Serializes this instance to [S].
  S serialize({AsyncTaskPlatform? platform}) {
    var data = this.data;
    if (noSerializationOnDartNative && (platform?.isNative ?? false)) {
      if (data is SerializableData && data.requiresSerializationToBeShared) {
        return data.serialize() as S;
      } else {
        return data as S;
      }
    } else if (serializer != null) {
      return serializer!(data);
    } else {
      return SerializableData.serializeGeneric(data);
    }
  }

  S? _serial;

  /// Serializes this instances to [S] using a cache for recurrent calls.
  S serializeCached({AsyncTaskPlatform? platform}) {
    _serial ??= serialize(platform: platform);
    return _serial!;
  }

  String? _signature;

  /// The signature of [data], computed only once.
  ///
  /// The signature is used to identify this shared object between threads/isolates.
  String get signature => _signature ??= computeSignature();

  /// The computation of [signature].
  String computeSignature() {
    if (signatureComputer != null) {
      return signatureComputer!(data);
    } else {
      return SerializableData.computeGenericSignature(data);
    }
  }

  String Function(D data)? signatureComputer;

  /// Dispose any cached computation, like [signature] and [serializeCached].
  void reset() {
    _signature = null;
    _serial = null;
  }
}

/// Collects [SharedData] execution information.
class AsyncExecutorSharedDataInfo {
  Set<String> sentSharedDataSignatures = {};

  Set<String> disposedSharedDataSignatures = {};

  bool get isEmpty =>
      sentSharedDataSignatures.isEmpty && disposedSharedDataSignatures.isEmpty;

  @override
  String toString() {
    if (isEmpty) {
      return 'AsyncExecutorSharedDataInfo{ empty }';
    }

    return 'AsyncExecutorSharedDataInfo{ '
        'sent: $sentSharedDataSignatures'
        ', disposed: $disposedSharedDataSignatures'
        ' }';
  }
}
