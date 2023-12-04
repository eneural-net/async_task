import 'dart:async';
import 'dart:typed_data';

import 'package:async_task/async_task.dart';
import 'package:collection/collection.dart';

/// extension for [AsyncTask] [Iterable].
extension AsyncTaskExtension<P, R, T extends AsyncTask<P, R>> on Iterable<T> {
  /// Returns a [List] of `idle` tasks.
  List<T> get idleTasks => where((t) => t.isIdle).toList();

  /// Returns a [List] of `submitted` tasks.
  List<T> get submittedTasks => where((t) => t.wasSubmitted).toList();

  /// Returns a [List] of `finished` tasks.
  List<T> get finishedTasks => where((t) => t.isFinished).toList();

  /// Returns a [List] of NOT `finished` tasks.
  List<T> get notFinishedTasks => where((t) => t.isNotFinished).toList();

  /// Returns a [List] of successful tasks.
  List<T> get successfulTasks => where((t) => t.isSuccessful).toList();

  /// Returns a [List] of tasks with errors.
  List<T> get errorTasks => where((t) => t.hasError).toList();

  /// Returns `true` if this collection has an idle task.
  bool get hasIdleTask => where((t) => t.isIdle).isNotEmpty;

  /// Returns `true` if this collection has a task with error.
  bool get hasTaskWithError => where((t) => t.hasError).isNotEmpty;

  /// Returns `true` if all tasks were submitted for execution.
  bool get allTasksSubmitted => where((t) => !t.wasSubmitted).isEmpty;

  /// Returns `true` if all tasks are finished.
  bool get allTasksFinished => where((t) => t.isNotFinished).isEmpty;

  /// Returns `true` if all tasks are finished and successful.
  bool get allTasksSuccessful => where((t) => !t.isSuccessful).isEmpty;

  /// Returns a [List] of tasks with [status].
  List<T> tasksWithStatus(AsyncTaskStatus status) =>
      where((t) => t.status == status).toList();

  /// Returns a [List] of the current results.
  List<R?> get tasksResults => map((t) => t.result).toList();

  /// Waits the tasks executions and returns a [List] of results.
  ///
  /// - [onlySubmitted] if true will wait only for `submitted` tasks.
  Future<List<R>> waitResults({bool onlySubmitted = false}) {
    var tasks = (onlySubmitted ? where((t) => t.wasSubmitted) : this);
    return Future.wait(tasks.map((t) => t.waitResult()));
  }

  /// Waits the tasks executions and returns a [List] of waited tasks.
  ///
  /// - [onlySubmitted] if true will wait only for `submitted` tasks.
  Future<List<T>> waitTasks({bool onlySubmitted = false}) async {
    var tasks = (onlySubmitted ? where((t) => t.wasSubmitted) : this).toList();
    await Future.wait(tasks.map((t) => t.waitResult()));
    return tasks;
  }

  /// Executes the tasks with [executor].
  List<Future<R>> executeTasks(AsyncExecutor executor) =>
      executor.executeAll(this);

  /// Executes the tasks with [executor], waits the executions and returns the results.
  Future<List<R>> executeAndWaitResults(AsyncExecutor executor) async {
    var tasks = toList();
    var futureResults = executor.executeAll(tasks);
    var results = Future.wait(futureResults);
    return results;
  }

  /// Executes the tasks with [executor], waits the executions and returns the tasks.
  Future<List<T>> executeAndWaitTasks(AsyncExecutor executor) async {
    var tasks = toList();
    var futureResults = executor.executeAll(this);
    await Future.wait(futureResults);
    return tasks;
  }
}

extension ListExtension<T> on List<T> {
  static final ListEquality _equality = ListEquality();

  Type get genericType => T;

  List<T> copy() => List<T>.from(this);

  List<T> deepCopy() =>
      map<T>((T e) => SerializableData.copyGeneric<T>(e)).toList();

  bool deepEquals(List<T> other) => _equality.equals(this, other);

  int computeHashcode() => _equality.hash(this);

  S serialize<S>() {
    if (this is List<int>) {
      return this as S;
    } else if (this is List<double>) {
      return this as S;
    } else if (this is List<num>) {
      return this as S;
    } else if (this is List<bool>) {
      return this as S;
    } else if (this is List<String>) {
      return this as S;
    } else {
      return map((e) => SerializableData.serializeGeneric(e)).toList() as S;
    }
  }
}

extension MapExtension<K, V> on Map<K, V> {
  static final MapEquality _equality = MapEquality();

  Type get keyType => K;

  Type get valueType => V;

  Map<K, V> copy() => Map.from(this);

  Map<K, V> deepCopy() => Map.from(map((key, value) => MapEntry(
      SerializableData.copyGeneric(key), SerializableData.copyGeneric(value))));

  bool deepEquals(Map<K, V> other) => _equality.equals(this, other);

  int computeHashcode() => _equality.hash(this);

  bool _isBasicType(Type type) {
    return type == int ||
        type == double ||
        type == num ||
        type == bool ||
        type == String;
  }

  S serialize<S>() {
    if (_isBasicType(keyType) && _isBasicType(valueType)) {
      return this as S;
    }

    return map((k, v) => MapEntry(SerializableData.serializeGeneric(k),
        SerializableData.serializeGeneric(v))) as S;
  }

  void removeAllKeys(Iterable<Object?> keys) {
    for (var k in keys) {
      remove(k);
    }
  }
}

extension SetExtension<T> on Set<T> {
  static final SetEquality _equality = SetEquality();

  Set<T> copy() => Set.from(this);

  Set<T> deepCopy() => Set.from(map(SerializableData.copyGeneric));

  bool deepEquals(Set<T> other) => _equality.equals(this, other);

  int computeHashcode() => _equality.hash(this);

  S serialize<S>() {
    if (this is Set<int>) {
      return this as S;
    } else if (this is Set<double>) {
      return this as S;
    } else if (this is Set<num>) {
      return this as S;
    } else if (this is Set<bool>) {
      return this as S;
    } else if (this is Set<String>) {
      return this as S;
    } else {
      return map((e) => SerializableData.serializeGeneric(e)).toSet() as S;
    }
  }
}

extension IterableExtension<T> on Iterable<T> {
  static final IterableEquality _equality = IterableEquality();

  List<T> copy() => List.from(this);

  List<T> deepCopy() => List.from(map(SerializableData.copyGeneric));

  bool deepEquals(Set<T> other) => _equality.equals(this, other);

  int computeHashcode() => _equality.hash(this);

  S serialize<S>() {
    if (this is Iterable<int>) {
      return this as S;
    } else if (this is Iterable<double>) {
      return this as S;
    } else if (this is Iterable<num>) {
      return this as S;
    } else if (this is Iterable<bool>) {
      return this as S;
    } else if (this is Iterable<String>) {
      return this as S;
    } else {
      return map((e) => SerializableData.serializeGeneric(e)).toList() as S;
    }
  }
}

extension ListIntExtension on List<int> {
  Int32x4List toInt32x4List() {
    var length = this.length;
    var size = length ~/ 4;
    if (size * 4 < length) ++size;

    var list = List.generate(size, (i) {
      var j = i * 4;
      var x = this[j];
      var y = this[++j];
      var z = this[++j];
      var w = this[++j];
      return Int32x4(x, y, z, w);
    });

    return Int32x4List.fromList(list);
  }

  Float32x4List toFloat32x4List() {
    var length = this.length;
    var size = length ~/ 4;
    if (size * 4 < length) ++size;

    var list = List.generate(size, (i) {
      var j = i * 4;
      var x = this[j].toDouble();
      var y = this[++j].toDouble();
      var z = this[++j].toDouble();
      var w = this[++j].toDouble();
      return Float32x4(x, y, z, w);
    });

    return Float32x4List.fromList(list);
  }
}

extension ListDoubleExtension on List<double> {
  Int32x4List toInt32x4List() {
    var length = this.length;
    var size = length ~/ 4;
    if (size * 4 < length) ++size;

    var list = List.generate(size, (i) {
      var j = i * 4;
      var x = this[j].toInt();
      var y = this[++j].toInt();
      var z = this[++j].toInt();
      var w = this[++j].toInt();
      return Int32x4(x, y, z, w);
    });

    return Int32x4List.fromList(list);
  }

  Float32x4List toFloat32x4List() {
    var length = this.length;
    var size = length ~/ 4;
    if (size * 4 < length) ++size;

    var list = List.generate(size, (i) {
      var j = i * 4;
      var x = this[j];
      var y = this[++j];
      var z = this[++j];
      var w = this[++j];
      return Float32x4(x, y, z, w);
    });

    return Float32x4List.fromList(list);
  }

  Float64x2List toFloat64x2List() {
    var length = this.length;
    var size = length ~/ 2;
    if (size * 4 < length) ++size;

    var list = List.generate(size, (i) {
      var j = i * 4;
      var x = this[j];
      var y = this[++j];
      return Float64x2(x, y);
    });

    return Float64x2List.fromList(list);
  }
}

extension ListFloat32x4Extension on List<Float32x4> {
  static List<double> entriesToDoubles(List<Float32x4> list) {
    final lastEntryIndex = list.length - 1;
    var entryIndex = 0;
    var entry = list.first;
    var entryCursor = -1;
    var valueCursor = -1;

    var fs = List.generate(list.length * 4, (i) {
      assert(++valueCursor == i);

      switch (++entryCursor) {
        case 0:
          {
            return entry.x;
          }
        case 1:
          {
            return entry.y;
          }
        case 2:
          {
            return entry.z;
          }
        case 3:
          {
            var w = entry.w;
            if (entryIndex < lastEntryIndex) {
              entry = list[++entryIndex];
              entryCursor = -1;
            }
            return w;
          }
        default:
          throw StateError('Invalid entryCursor: $entryCursor');
      }
    });

    return fs;
  }

  static List<int> entriesToInts(List<Float32x4> list) {
    final lastEntryIndex = list.length - 1;
    var entryIndex = 0;
    var entry = list.first;
    var entryCursor = -1;
    var valueCursor = -1;

    var fs = List.generate(list.length * 4, (i) {
      assert(++valueCursor == i);

      switch (++entryCursor) {
        case 0:
          {
            return entry.x.toInt();
          }
        case 1:
          {
            return entry.y.toInt();
          }
        case 2:
          {
            return entry.z.toInt();
          }
        case 3:
          {
            var w = entry.w;
            if (entryIndex < lastEntryIndex) {
              entry = list[++entryIndex];
              entryCursor = -1;
            }
            return w.toInt();
          }
        default:
          throw StateError('Invalid entryCursor: $entryCursor');
      }
    });

    return fs;
  }

  List<double> toDoubles() => entriesToDoubles(this);

  List<int> toInts() => entriesToInts(this);

  S serialize<S>() => toDoubles() as S;
}

extension Float32x4ListExtension on Float32x4List {
  List<double> toDoubles() => ListFloat32x4Extension.entriesToDoubles(this);

  List<int> toInts() => ListFloat32x4Extension.entriesToInts(this);

  S serialize<S>() => toDoubles() as S;
}

extension ListInt32x4Extension on List<Int32x4> {
  static List<int> entriesToInts(List<Int32x4> list) {
    final lastEntryIndex = list.length - 1;
    var entryIndex = 0;
    var entry = list.first;
    var entryCursor = -1;
    var valueCursor = -1;

    var fs = List.generate(list.length * 4, (i) {
      assert(++valueCursor == i);

      switch (++entryCursor) {
        case 0:
          {
            return entry.x;
          }
        case 1:
          {
            return entry.y;
          }
        case 2:
          {
            return entry.z;
          }
        case 3:
          {
            var w = entry.w;
            if (entryIndex < lastEntryIndex) {
              entry = list[++entryIndex];
              entryCursor = -1;
            }
            return w;
          }
        default:
          throw StateError('Invalid entryCursor: $entryCursor');
      }
    });

    return fs;
  }

  static List<double> entriesToDoubles(List<Int32x4> list) {
    final lastEntryIndex = list.length - 1;
    var entryIndex = 0;
    var entry = list.first;
    var entryCursor = -1;
    var valueCursor = -1;

    var fs = List.generate(list.length * 4, (i) {
      assert(++valueCursor == i);

      switch (++entryCursor) {
        case 0:
          {
            return entry.x.toDouble();
          }
        case 1:
          {
            return entry.y.toDouble();
          }
        case 2:
          {
            return entry.z.toDouble();
          }
        case 3:
          {
            var w = entry.w;
            if (entryIndex < lastEntryIndex) {
              entry = list[++entryIndex];
              entryCursor = -1;
            }
            return w.toDouble();
          }
        default:
          throw StateError('Invalid entryCursor: $entryCursor');
      }
    });

    return fs;
  }

  List<int> toInts() => entriesToInts(this);

  List<double> toDoubles() => entriesToDoubles(this);

  S serialize<S>() => toInts() as S;
}

extension Int32x4ListExtension on Int32x4List {
  List<int> toInts() => ListInt32x4Extension.entriesToInts(this);

  List<double> toDoubles() => ListInt32x4Extension.entriesToDoubles(this);

  S serialize<S>() => toInts() as S;
}

extension ListFloat64x2Extension on List<Float64x2> {
  static List<double> entriesToDoubles(List<Float64x2> list) {
    final lastEntryIndex = list.length - 1;
    var entryIndex = 0;
    var entry = list.first;
    var entryCursor = -1;
    var valueCursor = -1;

    var fs = List.generate(list.length * 2, (i) {
      assert(++valueCursor == i);

      switch (++entryCursor) {
        case 0:
          {
            return entry.x;
          }
        case 1:
          {
            var y = entry.y;
            if (entryIndex < lastEntryIndex) {
              entry = list[++entryIndex];
              entryCursor = -1;
            }
            return y;
          }
        default:
          throw StateError('Invalid entryCursor: $entryCursor');
      }
    });

    return fs;
  }

  static List<int> entriesToInts(List<Float64x2> list) {
    final lastEntryIndex = list.length - 1;
    var entryIndex = 0;
    var entry = list.first;
    var entryCursor = -1;
    var valueCursor = -1;

    var fs = List.generate(list.length * 2, (i) {
      assert(++valueCursor == i);

      switch (++entryCursor) {
        case 0:
          {
            return entry.x.toInt();
          }
        case 1:
          {
            var y = entry.y;
            if (entryIndex < lastEntryIndex) {
              entry = list[++entryIndex];
              entryCursor = -1;
            }
            return y.toInt();
          }
        default:
          throw StateError('Invalid entryCursor: $entryCursor');
      }
    });

    return fs;
  }

  List<double> toDoubles() => entriesToDoubles(this);

  List<int> toInts() => entriesToInts(this);

  S serialize<S>() => toDoubles() as S;
}

extension Float64x2ListExtension on Float64x2List {
  List<double> toDoubles() => ListFloat64x2Extension.entriesToDoubles(this);

  List<int> toInts() => ListFloat64x2Extension.entriesToInts(this);

  S serialize<S>() => toDoubles() as S;
}

extension SuppressFutureUnhandledErrorExtension on Future {
  void suppressUnhandledError() {
    then((_) => null, onError: (_) => null);
  }
}
