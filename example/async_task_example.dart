import 'dart:async';
import 'dart:math';

import 'package:async_task/async_task.dart';

void main() async {
  // A list of known primes, shared between tasks.
  // `SharedData` instances are handled in an efficient way,
  // since they are sent to threads/isolates only once, and not per task.
  var knownPrimes = SharedData<List<int>, List<int>>([2, 3, 4, 5]);

  // The tasks to execute:
  var tasks = [
    PrimeChecker(0, knownPrimes), // Not Prime
    PrimeChecker(1, knownPrimes), // Not Prime
    PrimeChecker(2, knownPrimes),
    PrimeChecker(5, knownPrimes),
    PrimeChecker(7, knownPrimes),
    PrimeChecker(11, knownPrimes),
    PrimeChecker(21, knownPrimes), // Not Prime
    PrimeChecker(31, knownPrimes),
    PrimeChecker(41, knownPrimes),
    PrimeChecker(51, knownPrimes), // Not Prime
    PrimeChecker(61, knownPrimes),
    PrimeChecker(71, knownPrimes),
    PrimeChecker(81, knownPrimes), // Not Prime
    PrimeChecker(8779, knownPrimes),
    PrimeChecker(1046527, knownPrimes),
    PrimeChecker(3139581, knownPrimes), // Not Prime
    PrimeChecker(16769023, knownPrimes),
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

  // Close the executor.
  await asyncExecutor.close();
}

// This top-level function returns the tasks types that will be registered
// for execution. Task instances are returned, but won't be executed and
// will be used only to identify the task type:
List<AsyncTask> _taskTypeRegister() =>
    [PrimeChecker(0, SharedData<List<int>, List<int>>([]))];

// A task that checks if a number is prime:
class PrimeChecker extends AsyncTask<int, bool> {
  // The number to check if is prime.
  final int n;

  // A list of known primes, shared between tasks.
  final SharedData<List<int>, List<int>> knownPrimes;

  PrimeChecker(this.n, this.knownPrimes);

  // Instantiates a `PrimeChecker` task with `parameters`.
  @override
  AsyncTask<int, bool> instantiate(int parameters, [SharedData? sharedData]) {
    return PrimeChecker(
        parameters, sharedData as SharedData<List<int>, List<int>>);
  }

  // The `SharedData` of this task.
  @override
  SharedData<List<int>, List<int>> sharedData() => knownPrimes;

  // Loads the `SharedData` from `serial`.
  @override
  SharedData<List<int>, List<int>> loadSharedData(dynamic serial) =>
      SharedData<List<int>, List<int>>(serial);

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

    // The pre-computed primes, optimizing this checking algorithm:
    if (knownPrimes.data.contains(n)) {
      return true;
    }

    // If a number N has a prime factor larger than `sqrt(N)`,
    // then it surely has a prime factor smaller `sqrt(N)`.
    // So it's sufficient to search for prime factors in the range [1,sqrt(N)]:
    var limit = (sqrt(n) + 1).toInt();

    for (var p = 2; p < limit; ++p) {
      if (n % p == 0) return false;
    }

    return true;
  }
}
