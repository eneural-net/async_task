import 'dart:async';

import 'package:async_task/async_task.dart';

void main() async {
  // The tasks to execute:
  var tasks = [
    PrimeChecker(0), // Not Prime
    PrimeChecker(1), // Not Prime
    PrimeChecker(2),
    PrimeChecker(5),
    PrimeChecker(7),
    PrimeChecker(11),
    PrimeChecker(21), // Not Prime
    PrimeChecker(31),
    PrimeChecker(41),
    PrimeChecker(51), // Not Prime
    PrimeChecker(61),
    PrimeChecker(71),
    PrimeChecker(81), // Not Prime
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
  // Enable logging output with execution information:
  asyncExecutor.logger.enabledExecution = true;

  // Execute all tasks:
  var executions = asyncExecutor.executeAll(tasks);

  // Wait tasks executions:
  await Future.wait(executions);

  for (var task in tasks) {
    var n = task.n; // Number to check for prime.
    var prime = task.result; // Task result: true if is prime.
    print('$n\t-> $prime \t $task');
  }
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
  AsyncTask<int, bool> instantiate(int parameters) {
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

    var limit = n ~/ 2;
    for (var p = 2; p < limit; ++p) {
      if (n % p == 0) return false;
    }

    return true;
  }
}
