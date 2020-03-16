## Open Distro for Elasticsearch Anomaly Detection

The Open Distro for Elasticsearch Anomaly Detection plugin enables you to leverage Machine Learning based algorithms to automatically detect anomalies as your log data is ingested. Combined with Alerting, you can monitor your data in near real time and automatically send alert notifications . With an intuitive Kibana interface and a powerful API, it is easy to set up, tune, and monitor your anomaly detectors.

## Highlights

Anomaly detection is using Random Cut Forest (RCF) algorithm for detecting anomalous data points.

You should use anomaly detection plugin with Open Distro Alerting plugin [1.2-alpha](https://github.com/opendistro-for-elasticsearch/alerting). You can also create a monitor based on anomaly detector. A scheduled monitor run will trigger the anomaly detection plugin and collecting anomalies to trigger alerts based on custom trigger conditions.
  
## Current Limitations
* This is alpha code.
* We inherit security features from Open Distro for Elasticsearch Alerting.
* We use Alerting alpha code branch.
* We will continuously add new unit test cases, but we don't have 100% unit test coverage for now. This is a great area for developers from the community to contribute and help improve test coverage.
* Please see documentation links and GitHub issues for other details.

## Documentation

Please see our [documentation](https://opendistro.github.io/for-elasticsearch-docs/).
  
## Setup

1. Checkout source code of this package from Github repo.
1. Launch Intellij IDEA, Choose Import Project and select the settings.gradle file in the root of this package.
1. To build from command line set JAVA_HOME to point to a JDK 12 before running ./gradlew

  * Unix System
    * export JAVA_HOME=jdk-install-dir: Replace jdk-install-dir by the JAVA_HOME directory of your system.
    * export PATH=$JAVA_HOME/bin:$PATH
  * Windows System
    * Find **My Computers** from file directory, right click and select **properties**.
    * Select the **Advanced** tab, select **Environment variables**.
    * Edit **JAVA_HOME** to path of where JDK software is installed.


## Build

This package uses the [Gradle](https://docs.gradle.org/current/userguide/userguide.html) build system. Gradle comes with excellent documentation that should be your first stop when trying to figure out how to operate or modify the build. we also use the Elastic build tools for Gradle. These tools are idiosyncratic and don't always follow the conventions and instructions for building regular Java code using Gradle. Not everything in this package will work the way it's described in the Gradle documentation. If you encounter such a situation, the Elastic build tools [source code](https://github.com/elastic/elasticsearch/tree/master/buildSrc/src/main/groovy/org/elasticsearch/gradle) is your best bet for figuring out what's going on.

Currently we just put RCF jar in lib as dependency. Plan to publish to Maven and we can import it later. Before publishing to Maven, you can still build this package directly and find source code in RCF Github package.

### Building from the command line

1. `./gradlew build` builds and tests
1. `./gradlew :run` launches a single node cluster with the AD plugin installed
1. `./gradlew :integTest` launches a single node cluster with the AD plugin installed and runs all integration tests
1. ` ./gradlew :integTest --tests="**.test execute foo"` runs a single integration test class or method
1. `./gradlew spotlessApply` formats code. And/or import formatting rules in `.eclipseformat.xml` with IDE.

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

#### Kill the nodes when you're done!

```
./gradlew stopMultiNode
```

## Known Issues
* We have a cold start period whenever we initialize a model, which could happen when we create a new detector or when the cluster restarts and models get restored from snapshots. Currently the detector always return errors during the cold start period. Please ignore these initial errors for now. We are actively working on the fix and will push in the next release.

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
