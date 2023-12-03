# async_task

[![pub package](https://img.shields.io/pub/v/async_task.svg?logo=dart&logoColor=00b9fc)](https://pub.dev/packages/async_task)
[![Null Safety](https://img.shields.io/badge/null-safety-brightgreen)](https://dart.dev/null-safety)
[![Dart CI](https://github.com/eneural-net/async_task/actions/workflows/dart.yml/badge.svg?branch=master)](https://github.com/eneural-net/async_task/actions/workflows/dart.yml)
[![GitHub Tag](https://img.shields.io/github/v/tag/eneural-net/async_task?logo=git&logoColor=white)](https://github.com/eneural-net/async_task/releases)
[![New Commits](https://img.shields.io/github/commits-since/eneural-net/async_task/latest?logo=git&logoColor=white)](https://github.com/eneural-net/async_task/network)
[![Last Commits](https://img.shields.io/github/last-commit/eneural-net/async_task?logo=git&logoColor=white)](https://github.com/eneural-net/async_task/commits/master)
[![Pull Requests](https://img.shields.io/github/issues-pr/eneural-net/async_task?logo=github&logoColor=white)](https://github.com/eneural-net/async_task/pulls)
[![Code size](https://img.shields.io/github/languages/code-size/eneural-net/async_task?logo=github&logoColor=white)](https://github.com/eneural-net/async_task)
[![License](https://img.shields.io/github/license/eneural-net/async_task?logo=open-source-initiative&logoColor=green)](https://github.com/eneural-net/async_task/blob/master/LICENSE)
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Feneural-net%2Fasync_task.svg?type=shield)](https://app.fossa.com/projects/git%2Bgithub.com%2Feneural-net%2Fasync_task?ref=badge_shield)

This package brings asynchronous tasks and parallel executors (similar to classic thread pools) for all Dart platforms
(JS/Web, Flutter, VM/Native) through transparent internal implementations, based on [dart:isolate][dart_isolate] or
only [dart:async][dart_async], without having to deal with the [Isolate] complexity.

[dart_async]: https://api.dart.dev/stable/2.13.1/dart-async/dart-async-library.html
[dart_isolate]: https://api.dart.dev/stable/2.13.1/dart-isolate/dart-isolate-library.html
[Isolate]: https://api.dart.dev/stable/2.12.4/dart-isolate/Isolate-class.html

## Motivation

Dart parallelism is based on asynchronous, non-blocking and thread-safe code. This creates a language that facilitates
the creation of safe concurrency code, but all running in the same thread, using only 1 thread/core of a device.

If you want to use more than 1 thread/core of a device, Dart VM/Native has [Isolate], but the paradigm is not easy to
use like classical Thread Pools, or like environments that shares all the memory/objects between threads.

This package was created to facilitate the creation and execution of multiple tasks asynchronously in all Dart
Platforms, avoiding platform specific codes by the developer.

## Usage

```dart
import 'dart:async';

import 'package:async_task/async_task.dart';

void main() async {
  // The tasks to execute:
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
    taskTypeRegister: _taskTypeRegister, // The top-level function to register the tasks types.
  );

  // Enable logging output:
  asyncExecutor.logger.enabled = true ;

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
    for (var p = 2; p <= limit; ++p) {
      if (n % p == 0) return false;
    }

    return true;
  }
}
```

Output:

```text
[INFO] Starting AsyncExecutor{ sequential: false, parallelism: 2, executorThread: _AsyncExecutorMultiThread{ totalThreads: 2, queue: 0 } }
[INFO] Starting _AsyncExecutorMultiThread{ totalThreads: 2, queue: 0 }
[INFO] Created _IsolateThread{ id: 2 ; registeredTasksTypes: [PrimeChecker] }
[INFO] Created _IsolateThread{ id: 1 ; registeredTasksTypes: [PrimeChecker] }
8779     -> true 	 PrimeChecker(8779)[finished]{ result: true ; executionTime: 2 ms }
1046527  -> true 	 PrimeChecker(1046527)[finished]{ result: true ; executionTime: 2 ms }
3139581  -> false 	 PrimeChecker(3139581)[finished]{ result: false ; executionTime: 0 ms }
16769023 -> true 	 PrimeChecker(16769023)[finished]{ result: true ; executionTime: 35 ms }
```

### parallelism

Note that the parameter `parallelism` at `AsyncExecutor` shouldn't be greater than the CPU cores
*(unless you really have a case that justifies a higher number)*.

The most common error is to put a high number for the parameter `parallelism` thinking that this will increase tasks
execution, but this actually  can reduce your application performance. Note that each allocated `Isolate`needs a CPU
core to execute, and a high number of `Isolate` instances running tasks will be a bottleneck in your system
(specially if you are demanding more `Isolate` than CPU cores). 

## SharedData

The class `SharedData` facilitates and optimizes data shared between tasks.
The main advantage of data encapsulated with `SharedData` is to avoid multiple
messages, with the same data, between threads/isolates, avoiding concurrency
performance issues and multiple duplicated objects in memory (a GC bottleneck).

Here's an example of a task using `SharedData`:

```dart
// A task that checks if a number is prime:
class PrimeChecker extends AsyncTask<int, bool> {
  // The number to check if is prime.
  final int n;

  // A list of known primes, shared between tasks.
  final SharedData<List<int>, List<int>> knownPrimes;

  PrimeChecker(this.n, this.knownPrimes);

  // Instantiates a `PrimeChecker` task with `parameters` and `sharedData`.
  @override
  PrimeChecker instantiate(int parameters, [Map<String, SharedData>? sharedData]) {
    return PrimeChecker(
      parameters,
      sharedData!['knownPrimes'] as SharedData<List<int>, List<int>>,
    );
  }

  // The `SharedData` of this task.
  @override
  Map<String, SharedData> sharedData() => {'knownPrimes': knownPrimes};

  // Loads the `SharedData` from `serial` for each key.
  @override
  SharedData<List<int>, List<int>> loadSharedData(String key, dynamic serial) {
    switch (key) {
      case 'knownPrimes':
        return SharedData<List<int>, List<int>>(serial);
      default:
        throw StateError('Unknown key: $key');
    }
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

    // The pre-computed primes, optimizing this checking algorithm:
    if (knownPrimes.data.contains(n)) {
      return true;
    }
    
    // It's sufficient to search for prime factors in the range [1,sqrt(N)]:
    var limit = (sqrt(n) + 1).toInt();

    for (var p = 2; p < limit; ++p) {
      if (n % p == 0) return false;
    }

    return true;
  }
}
```

The field `knownPrimes` above will be shared between tasks. In platforms with support
for [dart:isolate][dart_isolate] `knownPrimes` will be sent through an [Isolate] port only once,
avoiding multiple copies and unnecessary memory allocations.

## AsyncTaskChannel

An `AsyncTask` can have a communication channel that can be used to send/received messages
during task execution.

To use a task channel just override `channelInstantiator`, than use `channelResolved()` inside
the task and `await channel()` outside it:

```dart
class YourTask extends AsyncTask<String, int> {
  // ...
  
  @override
  AsyncTaskChannel? channelInstantiator() => AsyncTaskChannel(); // You can extend `AsyncTaskChannel` class if needed.

  // ...

  @override
  FutureOr<int> run() async {
    // ...
    
    // get the resolved channel:
    var channel = channelResolved()!; // The channel is always resolved inside `run()`.
    
    // Send and wait a message in the channel:
    channel.send('some message');
    var result = await channel.waitMessage<String>();
    
    // Send and wait in a single method:
    var result = await channel.sendAndWaitResponse<String, String>('some message');

    // Read a message if available in the queue (non-blocking):
    var msgOptional = channel.readMessage<String>();
    
    // ...
  }
}

```

Outside communication with the task:
```dart
void main() async {
  // ...
  
  // Execute the task:
  asyncExecutor.execute(task);
  
  // ...
  
  // Wait for the channel to be resolved:
  var channel = (await task.channel())!;
  
  // Wait for a message:
  var msg = await channel.waitMessage();
  
  // process msg...
  
  // Send a response:
  channel.send('Some response');
  
  // ...
}
```

An `AsyncTaskChannel` is automatically closed when a task finishes (returns its result).

## Source

The official source code is [hosted @ GitHub][github_async_task]:

- https://github.com/eneural-net/async_task

[github_async_task]: https://github.com/eneural-net/async_task

# Features and bugs

Please file feature requests and bugs at the [issue tracker][tracker].

# Package `shared_map`

See also the package [shared_map] for a way to transparently share data/objects between `Isolate`s.

[shared_map]: https://pub.dev/packages/shared_map

## Contribution

Any help from the open-source community is always welcome and needed:

- **Have an issue?**
  Please fill a bug report 👍.
- **Feature?**
  Request with use cases 🤝.
- **Like the project?**
  Promote, post, or donate 😄.
- **Are you a developer?**
  Fix a bug, add a feature, or improve tests 🚀.
- **Already helped?**
  Many thanks from me, the contributors and all project users 👏👏👏!

*Contribute an hour and inspire others to do the same.*

[tracker]: https://github.com/eneural-net/async_task/issues

# Author

Graciliano M. Passos: [gmpassos@GitHub][github].

[github]: https://github.com/gmpassos

## Sponsor

Don't be shy, show some love, and become our [GitHub Sponsor][github_sponsors].
Your support means the world to us, and it keeps the code caffeinated! ☕✨

Thanks a million! 🚀😄

[github_sponsors]: https://github.com/sponsors/gmpassos

## License

[Apache License - Version 2.0][apache_license]

[apache_license]: https://www.apache.org/licenses/LICENSE-2.0.txt


[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Feneural-net%2Fasync_task.svg?type=large)](https://app.fossa.com/projects/git%2Bgithub.com%2Feneural-net%2Fasync_task?ref=badge_large)