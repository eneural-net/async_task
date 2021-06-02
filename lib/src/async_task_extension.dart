import 'package:async_task/async_task.dart';

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
