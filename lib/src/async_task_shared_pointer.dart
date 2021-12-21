import 'dart:async';
import 'dart:ffi';
import 'dart:typed_data';

import 'package:ffi/ffi.dart';

import 'async_task_shared_data.dart';

/// A [SharedPointerBytes] mapped to a [Uint8List] of fixed [length].
class SharedPointerUInt8List extends SharedPointerBytes<int> {
  SharedPointerUInt8List(int length) : super(length, 1, true);

  SharedPointerUInt8List.fromAddress(SharedPointerAddress address)
      : super.fromAddress(address.parameters![0], 1, true, address);

  @override
  SharedPointerUInt8List copy() {
    return SharedPointerUInt8List.fromAddress(address);
  }

  @override
  SharedPointerUInt8List deserialize(SharedPointerAddress serial) {
    return serial.getPointer() as SharedPointerUInt8List;
  }

  @override
  SharedPointerUInt8List deserializeData(SharedPointerAddress serial) {
    return serial.getPointer() as SharedPointerUInt8List;
  }

  @override
  SharedPointerUInt8List instantiate(SharedPointer<int> data) {
    return data as SharedPointerUInt8List;
  }

  @override
  int? readFromByteData(ByteData byteData, int offset) =>
      byteData.getUint8(offset);

  @override
  void writeToByteData(ByteData byteData, int offset, int value) =>
      byteData.setUint8(offset, value);

  Uint8List view() => _data.buffer.asUint8List(_data.offsetInBytes, length);

  Uint8List sublist(int start, [int? end]) =>
      _data.sublist(start, end ?? length);
}

/// A [SharedPointerBytes] mapped to a [Uint32List] of fixed [length].
class SharedPointerUInt32List extends SharedPointerBytes<int> {
  SharedPointerUInt32List(int length) : super(length * 4, 4, true);

  SharedPointerUInt32List.fromAddress(SharedPointerAddress address)
      : super.fromAddress(address.parameters![0], 4, true, address);

  @override
  SharedPointerUInt32List copy() {
    return SharedPointerUInt32List.fromAddress(address);
  }

  @override
  SharedPointerUInt32List deserialize(SharedPointerAddress serial) {
    return serial.getPointer() as SharedPointerUInt32List;
  }

  @override
  SharedPointerUInt32List deserializeData(SharedPointerAddress serial) {
    return serial.getPointer() as SharedPointerUInt32List;
  }

  @override
  SharedPointerUInt32List instantiate(SharedPointer<int> data) {
    return data as SharedPointerUInt32List;
  }

  @override
  int? readFromByteData(ByteData byteData, int offset) =>
      byteData.getUint32(offset, Endian.host);

  @override
  void writeToByteData(ByteData byteData, int offset, int value) =>
      byteData.setUint32(offset, value, Endian.host);

  Uint32List view() {
    return _data.buffer.asUint32List(_data.offsetInBytes, length);
  }

  Uint32List sublist(int start, [int? end]) =>
      Uint32List.fromList(view().sublist(start, end ?? length));
}

/// A [SharedPointerBytes] mapped to a [Uint64List] of fixed [length].
class SharedPointerUInt64List extends SharedPointerBytes<int> {
  SharedPointerUInt64List(int length) : super(length * 8, 8, true);

  SharedPointerUInt64List.fromAddress(SharedPointerAddress address)
      : super.fromAddress(address.parameters![0], 8, true, address);

  @override
  SharedPointerUInt64List copy() {
    return SharedPointerUInt64List.fromAddress(address);
  }

  @override
  SharedPointerUInt64List deserialize(SharedPointerAddress serial) {
    return serial.getPointer() as SharedPointerUInt64List;
  }

  @override
  SharedPointerUInt64List deserializeData(SharedPointerAddress serial) {
    return serial.getPointer() as SharedPointerUInt64List;
  }

  @override
  SharedPointerUInt64List instantiate(SharedPointer<int> data) {
    return data as SharedPointerUInt64List;
  }

  @override
  int? readFromByteData(ByteData byteData, int offset) =>
      byteData.getUint64(offset, Endian.host);

  @override
  void writeToByteData(ByteData byteData, int offset, int value) =>
      byteData.setUint64(offset, value, Endian.host);

  /// Returns an [Uint64List] view of this [Pointer] buffer.
  Uint64List view() {
    return _data.buffer.asUint64List(_data.offsetInBytes, length);
  }

  /// Returns an [Uint64List] sub-list of this [Pointer] buffer,
  /// starting at [start] and ending at [end] (default to [length]).
  Uint64List sublist(int start, [int? end]) =>
      Uint64List.fromList(view().sublist(start, end ?? length));
}

/// A [SharedPointerBytes] mapped to a single [Uint32].
class SharedPointerUint32 extends SharedPointerBytes<int> {
  SharedPointerUint32() : super(4, 4, true);

  SharedPointerUint32.fromAddress(SharedPointerAddress address)
      : super.fromAddress(4, 4, true, address);

  @override
  SharedPointerUint32 copy() {
    return SharedPointerUint32.fromAddress(address);
  }

  @override
  SharedPointerUint32 deserialize(SharedPointerAddress serial) {
    return serial.getPointer() as SharedPointerUint32;
  }

  @override
  SharedPointerUint32 deserializeData(SharedPointerAddress serial) {
    return serial.getPointer() as SharedPointerUint32;
  }

  @override
  SharedPointerUint32 instantiate(SharedPointer<int> data) {
    return data as SharedPointerUint32;
  }

  @override
  int? readFromByteData(ByteData byteData, int offset) =>
      byteData.getUint32(offset, Endian.host);

  @override
  void writeToByteData(ByteData byteData, int offset, int value) =>
      byteData.setUint32(offset, value, Endian.host);
}

/// A [SharedPointerBytes] mapped to a single [Uint64].
class SharedPointerUint64 extends SharedPointerBytes<int> {
  SharedPointerUint64() : super(8, 8, true);

  SharedPointerUint64.fromAddress(SharedPointerAddress address)
      : super.fromAddress(8, 8, true, address);

  @override
  SharedPointerUint64 copy() {
    return SharedPointerUint64.fromAddress(address);
  }

  @override
  SharedPointerUint64 deserialize(SharedPointerAddress serial) {
    return serial.getPointer() as SharedPointerUint64;
  }

  @override
  SharedPointerUint64 deserializeData(SharedPointerAddress serial) {
    return serial.getPointer() as SharedPointerUint64;
  }

  @override
  SharedPointerUint64 instantiate(SharedPointer<int> data) {
    return data as SharedPointerUint64;
  }

  @override
  int? readFromByteData(ByteData byteData, int offset) =>
      byteData.getUint64(offset);

  @override
  void writeToByteData(ByteData byteData, int offset, int value) =>
      byteData.setUint64(offset, value);
}

typedef SharedPointerMapWriter<T> = void Function(
    ByteData byteData, int offset, T value);
typedef SharedPointerMapReader<T> = T? Function(ByteData byteData, int offset);

/// A [SharedPointerBytes] mapped to generic [reader]/[writer] functions.
class SharedPointerMapped<T> extends SharedPointerBytes<T> {
  /// The writer [Function].
  final SharedPointerMapWriter<T> writer;

  /// The reader [Function].
  final SharedPointerMapReader<T> reader;

  SharedPointerMapped(
      int allocatedBytesLength, int bytesPerValue, this.writer, this.reader)
      : super(allocatedBytesLength, bytesPerValue, true);

  SharedPointerMapped.fromAddress(int allocatedBytesLength, int bytesPerValue,
      this.writer, this.reader, SharedPointerAddress address)
      : super.fromAddress(allocatedBytesLength, bytesPerValue, true, address);

  @override
  SharedPointerMapped<T> copy() {
    return SharedPointerMapped.fromAddress(
        allocatedBytesLength, bytesPerValue, writer, reader, address);
  }

  @override
  SharedPointerMapped<T> deserialize(SharedPointerAddress serial) {
    return serial.getPointer() as SharedPointerMapped<T>;
  }

  @override
  SharedPointerMapped<T> deserializeData(SharedPointerAddress serial) {
    return serial.getPointer() as SharedPointerMapped<T>;
  }

  @override
  SharedPointerMapped<T> instantiate(SharedPointer<T> data) {
    return data as SharedPointerMapped<T>;
  }

  @override
  T? readFromByteData(ByteData byteData, int offset) =>
      reader(byteData, offset);

  @override
  void writeToByteData(ByteData byteData, int offset, T value) =>
      writer(byteData, offset, value);
}

/// A [SharedPointer] that allocates a bytes (fixed [length]) that can be mapped to another type,
/// like [Uint32List] or [Uint32].
///
/// See [SharedPointerUInt8List], [SharedPointerUInt32List], [SharedPointerUInt64List],
/// [SharedPointerUint32], [SharedPointerUint64].
abstract class SharedPointerBytes<T> extends SharedPointer<T> {
  /// Amount of bytes allocated by this instance.
  final int allocatedBytesLength;

  /// Amount of bytes per value of this implementation.
  final int bytesPerValue;

  /// If `true` will initialize the instance with zeroes, removing any garbage
  /// in the allocated memory.
  final bool initializeZeroed;

  /// Returns the fixed length of elements in this instance.
  int length;

  late final Pointer<Uint8> _pointer;
  late final Uint8List _data;
  late final ByteData _byteData;

  SharedPointerBytes(
      this.allocatedBytesLength, this.bytesPerValue, this.initializeZeroed)
      : length = allocatedBytesLength ~/ bytesPerValue;

  SharedPointerBytes.fromAddress(this.allocatedBytesLength, this.bytesPerValue,
      this.initializeZeroed, SharedPointerAddress address)
      : length = allocatedBytesLength ~/ bytesPerValue,
        super.fromAddress(address);

  /// Returns `true` if this instance is of [length] `0`.
  bool get isEmpty => length == 0;

  /// Returns `true` if this instance is NOT of [length] `0`.
  bool get isNotEmpty => length > 0;

  @override
  Endian get endian => Endian.host;

  bool _initializer = false;

  @override
  bool get isInitializer => _initializer;

  @override
  SharedPointerAddress _initialize() {
    _initializer = true;
    _pointer = malloc.allocate<Uint8>(sizeOf<Uint8>() * allocatedBytesLength);
    return _initializeFromPointer();
  }

  @override
  SharedPointerAddress _initializeFromAddress(SharedPointerAddress address) {
    _pointer = Pointer.fromAddress(address.memoryAddress);
    return _initializeFromPointer();
  }

  SharedPointerAddress _initializeFromPointer() {
    _data = _pointer.asTypedList(allocatedBytesLength);
    _byteData =
        _data.buffer.asByteData(_data.offsetInBytes, _data.lengthInBytes);

    if (initializeZeroed && _initializer) {
      for (var i = 0; i < allocatedBytesLength; ++i) {
        _byteData.setUint8(i, 0);
      }
    }

    return SharedPointerAddress(
        _pointer.address, runtimeType, [allocatedBytesLength, bytesPerValue]);
  }

  /// Implementation to write a [value] to the [byteData] of this instance [Pointer].
  void writeToByteData(ByteData byteData, int offset, T value);

  /// Implementation to read a [value] from the [byteData] of this instance [Pointer].
  T? readFromByteData(ByteData byteData, int offset);

  @override
  void write(T value) {
    checkClosed();
    writeToByteData(_byteData, 0, value);
  }

  @override
  T? read() {
    checkClosed();
    return readFromByteData(_byteData, 0);
  }

  /// Writes a [value] at [index].
  ///
  /// The bytes offset is calculated by [index] `*` [bytesPerValue].
  void writeAt(int index, T value) {
    checkClosed();
    var offset = index * bytesPerValue;
    writeToByteData(_byteData, offset, value);
  }

  /// Reads a [value] from [index].
  ///
  /// The bytes offset is calculated by [index] `*` [bytesPerValue].
  T? readAt(int index) {
    checkClosed();
    var offset = index * bytesPerValue;
    return readFromByteData(_byteData, offset);
  }

  @override
  void free() {
    if (isClosed) return;
    super.free();
    if (isInitializer) {
      malloc.free(_pointer);
    }
  }
}

/// Base class for a `dart:ffi` [Pointer] that can be shared between `Isolate`s.
///
/// To share this points just send to the other `Isolate` the [SharedPointer.address].
/// Then in the other [Isolate] side just call [SharedPointerAddress.getPointer],
/// to have the working [SharedPointer] instance.
///
/// Note that a [ffi.Pointer] originally can't shared between [Isolate]s, but
/// sharing the pointer address allows the same memory are to be acessed
/// simultaneously by different `Isolate`. For now there's no `mutex` implementation
/// to allow control of the owner `thread` of the [SharedPointer].
abstract class SharedPointer<T>
    implements SerializableData<SharedPointer<T>, SharedPointerAddress> {
  late final SharedPointerAddress address;

  SharedPointer() {
    address = _initialize();
  }

  SharedPointer.fromAddress(this.address) {
    var initAddress = _initializeFromAddress(address);
    if (initAddress != address) {
      throw StateError(
          "Initialization returned a different address: $initAddress != $address");
    }
  }

  @override
  bool get requiresSerializationToBeShared => true;

  /// The `endianness` of this pointer.
  Endian get endian;

  /// Returns `true` if this instance is the initializer instance, that actually
  /// allocated the pointer. Only the initializer instance can [free] the pointer.
  bool get isInitializer;

  /// Implementation of the initialization of the pointer.
  SharedPointerAddress _initialize();

  /// Implementation of the initialization of the pointer from an [address] (for an already allocated pointer).
  SharedPointerAddress _initializeFromAddress(SharedPointerAddress address);

  /// Writes a [value] to the [Pointer] memory.
  void write(T value);

  /// Reads a [value] from the [Pointer] memory.
  T? read();

  /// Reads a new [value], different from [lastValue], performing a pooling with
  /// [interval] (default: 10ms) and a [timeout] (default: 10s) that checks
  /// for a new value if needed.
  FutureOr<T?> readNewValue(
      {T? lastValue, Duration? interval, Duration? timeout}) {
    var value = read();
    if (value != lastValue || lastValue == null) return value;

    interval ??= Duration(milliseconds: 10);
    timeout ??= Duration(seconds: 10);
    return _readNewValueImpl(lastValue, interval, timeout);
  }

  Future<T?> _readNewValueImpl(
      T lastValue, Duration interval, Duration timeout) async {
    var init = DateTime.now();
    while (true) {
      await Future.delayed(interval);

      var value = read();
      if (value != lastValue) return value;

      var elapsedTime = DateTime.now().difference(init);
      if (elapsedTime > timeout) return null;
    }
  }

  bool _closed = false;

  /// Returns `true` if this instance is closed (in the current [Isolate]).
  bool get isClosed => _closed;

  /// Checks if the instance is NOT closed, or throws a [StateError].
  void checkClosed() {
    if (_closed) {
      throw StateError("Instance already closed: $this");
    }
  }

  /// Closes this instance and calls `malloc.free` for the [Pointer] if this is the
  /// initializer instance. See [isInitializer].
  void free() {
    _closed = true;
  }

  @override
  String computeSignature() {
    return 'SharedPointer>${address.computeSignature()}';
  }

  @override
  SharedPointerAddress serialize() => address;
}

/// Represents a [SharedPointer.address], with its [memoryAddress], [type]
/// and [parameters] needed to re-instantiate a [SharedPointer].
///
/// **This is the actual instance that is shared between [Isolate]s.**
///
/// The method [getPointer] is capable to recreate the [SharedPointer] associated
/// with this instance.
class SharedPointerAddress
    implements SerializableData<SharedPointerAddress, List> {
  /// The real memory address for the associated [SharedPointer]'s [Pointer].
  final int memoryAddress;

  /// The [SharedPointer] [Type] of this address.
  final Type? type;

  /// The parameters need for [getPointer] recreate the [SharedPointer] instance.
  final List? parameters;

  SharedPointerAddress(this.memoryAddress, [this.type, this.parameters]);

  @override
  bool get requiresSerializationToBeShared => false;

  /// Returns the [SharedPointer] for [memoryAddress] and [type].
  SharedPointer<T> getPointer<T>() {
    var type = this.type;
    if (type == null) {
      throw StateError("Null type. Can't automatically handle pointer.");
    } else if (type == SharedPointerUint32) {
      return SharedPointerUint32.fromAddress(this) as SharedPointer<T>;
    } else if (type == SharedPointerUint64) {
      return SharedPointerUint64.fromAddress(this) as SharedPointer<T>;
    } else if (type == SharedPointerUInt8List) {
      return SharedPointerUInt8List.fromAddress(this) as SharedPointer<T>;
    } else if (type == SharedPointerUInt32List) {
      return SharedPointerUInt32List.fromAddress(this) as SharedPointer<T>;
    } else if (type == SharedPointerUInt64List) {
      return SharedPointerUInt64List.fromAddress(this) as SharedPointer<T>;
    } else {
      throw StateError("Can't automatically handle pointer type: $type");
    }
  }

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is SharedPointerAddress &&
          runtimeType == other.runtimeType &&
          memoryAddress == other.memoryAddress;

  @override
  int get hashCode => memoryAddress.hashCode;

  @override
  String toString() {
    return 'SharedPointerAddress{ #$memoryAddress }';
  }

  @override
  String computeSignature() {
    return 'SharedPointerAddress>$type>$memoryAddress>$parameters';
  }

  @override
  SharedPointerAddress copy() {
    var p = parameters;
    if (p != null) {
      p = List.from(p);
    }
    return SharedPointerAddress(memoryAddress, type, p);
  }

  @override
  SharedPointerAddress deserialize(List serial) {
    return SharedPointerAddress(serial[0], serial[1], serial[2]);
  }

  @override
  SharedPointerAddress deserializeData(List serial) {
    return SharedPointerAddress(serial[0], serial[1], serial[2]);
  }

  @override
  SerializableData<SharedPointerAddress, List> instantiate(
      SharedPointerAddress data) {
    return data;
  }

  @override
  List serialize() => [type, memoryAddress, parameters];
}
