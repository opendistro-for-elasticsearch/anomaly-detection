ElasticSearch Plugins for anomaly detection.

**NOTE**: This is a draft version which doesn't include everything and may contain errors. The code is just used to build a rough plugin. Feel free to restructure and build your own code. Use "Apache License Version 2.0" here just as a place holder. Will replace it once we decide which license to use.

Developing
--

## Setup

1. Checkout this package from version control. 
1. Launch Intellij IDEA, Choose Import Project and select the `settings.gradle` file in the root of this package. 
1. To build from command line set `JAVA_HOME` to point to a JDK >=12 before running `./gradlew`

## Build

### Building from command line

1. `./gradlew build` builds and tests
1. `./gradlew :run` launches a single node cluster with the AD plugin installed
1. `./gradlew :integTest` launches a single node cluster with the AD plugin installed and runs all integ tests
1. ` ./gradlew :integTest --tests="**.test execute foo"` runs a single integ test class or method

When launching a cluster using one of the above commands logs are placed in `/build/cluster/run node0/elasticsearch-<version>/logs`. Though the logs are teed to the console, in practices it's best to check the actual log file.

### Building from the IDE
The only IDE we support is IntelliJ IDEA.  It's free, it's open source, it works. The gradle tasks above can also be launched from IntelliJ's Gradle toolbar and the extra parameters can be passed in via the Launch Configurations VM arguments. 

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

