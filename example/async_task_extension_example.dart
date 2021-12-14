import 'dart:math';

import 'package:async_task/async_task.dart';
import 'package:async_task/async_task_extension.dart';

void main() async {
  // Tasks to execute:
  var tasks = [
    PrimeChecker(8779),
    PrimeChecker(1046527),
    PrimeChecker(3139581), // Not Prime
    PrimeChecker(16769023),
  ];

  // Instantiate the task executor:
  var asyncExecutor = AsyncExecutor(
    sequential: false, // Non-sequential tasks.
    parallelism: 2, // Concurrency with 2 threads.
    taskTypeRegister:
        _taskTypeRegister, // The top-level function to register the tasks types.
  );

  // Enable logging output:
  asyncExecutor.logger.enabled = true;
  asyncExecutor.logger.enabledExecution = true;

  // Execute and wait tasks results:
  await tasks.executeAndWaitTasks(asyncExecutor);

  // List of tasks with errors:
  var errorTasks = tasks.errorTasks;

  for (var task in errorTasks) {
    print('ERROR> $task');
  }

  // List of finished tasks
  var finishedTasks = tasks.finishedTasks;

  for (var task in finishedTasks) {
    var n = task.n;
    var prime = task.result;
    print('$n\t-> $prime \t $task');
  }

  // Close the executor.
  await asyncExecutor.close();
}

// This top-level function returns the tasks types that will be registered
// for execution. Task instances are returned, but won't be executed and
// will be used only to identify the task type:
List<AsyncTask> _taskTypeRegister() => [PrimeChecker(0)];

// A task that checks if a number is prime:
class PrimeChecker extends AsyncTask<int, bool> {
  // The number to check if is prime.
  final int n;

  PrimeChecker(this.n);

  // Instantiates a `PrimeChecker` task with `parameters`.
  @override
  AsyncTask<int, bool> instantiate(int parameters,
      [Map<String, SharedData>? sharedData]) {
    return PrimeChecker(parameters);
  }

  // The parameters of this task:
  @override
  int parameters() {
    return n;
  }

  // Runs the task code:
  @override
  FutureOr<bool> run() {
    return isPrime(n);
  }

  // A simple prime check function:
  bool isPrime(int n) {
    if (n < 2) return false;

    // It's sufficient to search for prime factors in the range [1,sqrt(N)]:
    var limit = (sqrt(n) + 1).toInt();

    for (var p = 2; p < limit; ++p) {
      if (n % p == 0) return false;
    }

    return true;
  }
}
