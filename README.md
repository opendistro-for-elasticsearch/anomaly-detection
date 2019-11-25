## Open Distro for Elasticsearch Anomaly Detection

This is an open source Apache-2 licensed ElasticSearch plugin for anomaly detection in ingested data streams.

**NOTE**: The code is in development currently and not distributed with Open Distro for Elasticsearch yet. So if you're looking for features that you need in anomaly detection for Elasticsearch, please file a request issue.

## Highlights

* Why would I use this code? How does the user benefit?
  
## Functionality
* List the project's components and the way they interact with one another
* What are the plugin functions and low-level data types
  
## Documentation

Please see our [documentation](https://opendistro.github.io/for-elasticsearch-docs/).
  
## Setup

1. Checkout this package from version control. 
1. Launch Intellij IDEA, Choose Import Project and select the `settings.gradle` file in the root of this package. 
1. To build from command line set `JAVA_HOME` to point to a JDK >=12 before running `./gradlew`

## Build

This package is organized into subprojects, most of which contribute JARs to the top-level plugin in the `anomaly-detection` subproject.

All subprojects in this package use the [Gradle](https://docs.gradle.org/current/userguide/userguide.html) build system. Gradle comes with excellent documentation that should be your first stop when trying to figure out how to operate or modify the build.

### Building from the command line

1. `./gradlew build` builds and tests
1. `./gradlew :run` launches a single node cluster with the AD plugin installed
1. `./gradlew :integTest` launches a single node cluster with the AD plugin installed and runs all integration tests
1. ` ./gradlew :integTest --tests="**.test execute foo"` runs a single integration test class or method

When launching a cluster using one of the above commands logs are placed in `/build/cluster/run node0/elasticsearch-<version>/logs`. Though the logs are teed to the console, in practices it's best to check the actual log file.

### Building from the IDE

Currently, the only IDE we support is IntelliJ IDEA.  It's free, it's open source, it works. The gradle tasks above can also be launched from IntelliJ's Gradle toolbar and the extra parameters can be passed in via the Launch Configurations VM arguments. 

### Debugging

Sometimes it's useful to attach a debugger to either the ES cluster or the integ tests to see what's going on. When running unit tests you can just hit 'Debug' from the IDE's gutter to debug the tests.  To debug code running in an actual server run:

```
./gradlew :integTest --debug-jvm # to start a cluster and run integ tests
OR
./gradlew :run --debug-jvm # to just start a cluster that can be debugged
```

The ES server JVM will launch suspended and wait for a debugger to attach to `localhost:8000` before starting the ES server.

To debug code running in an integ test (which exercises the server from a separate JVM) run:

```
./gradlew -Dtest.debug :integTest 
```

The **test runner JVM** will start suspended and wait for a debugger to attach to `localhost:5005` before running the tests.

### Advanced: Launching multi node clusters locally

Sometimes you need to launch a cluster with more than one ES server process. The `startMultiNode` tasks help with this.

#### All nodes are started and stopped together

If you need a multi node cluster where all nodes are started together use: 

```
./gradlew -PnumNodes=2 startMultiNode # to launch 2 nodes

```

If you need a single node cluster use:

```
./gradlew startMultiNode 

```

#### Nodes join and leave the cluster independently

If you need a multi node cluster (up to 3 nodes) where you'd like to be able to add and kill each node independently use:

```
./gradlew startSingleNode0
./gradlew startSingleNode1
./gradlew startSingleNode2
```

#### Kill the nodes when you're done!!

```
./gradlew stopMultiNode
```

## Interested in contributing to the Anomaly Detection plugin

We welcome you to get involved in development, documentation, testing the anomaly detection plugin. See our [contribution guidelines](https://opendistro.github.io/for-elasticsearch/blob/development/CONTRIBUTING.md) and join in.

## Code of Conduct

This project has adopted an [Open Source Code of Conduct](https://opendistro.github.io/for-elasticsearch/codeofconduct.html).


## Security issue notifications

If you discover a potential security issue in this project we ask that you notify AWS/Amazon Security via our [vulnerability reporting page](http://aws.amazon.com/security/vulnerability-reporting/). Please do **not** create a public GitHub issue.


## Licensing

See the [LICENSE](./LICENSE.txt) file for our project's licensing. We will ask you to confirm the licensing of your contribution.


## Copyright

Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
