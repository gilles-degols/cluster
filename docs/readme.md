# Purpose

The cluster library provides a simple way to create a system with *High Availability* & *Redundancy*  & *Scalability* in mind. As it is build on top of the [election library](https://github.com/gilles-degols/election), this library only need to be in charge of the scalability part to easily design major distributed systems based on actor systems. Akka cluster is generally used for the scalability part, but it does not provide high availability by itself (in specific cases, the [system can be stuck](https://doc.akka.io/docs/akka/2.5/cluster-usage.html#joining-configured-seed-nodes) if the processes do not restart in the correct order). 

With this library you are also able to easily implement custom load balancers depending on your need. It also allows having only one cluster management of completely different processes running fine together under the same Cluster Manager. So you can add at any time 2 new processes, only able to do a few tasks, and the existing manager will accept them and order them around.

# Requirements

RAM: \~100MB, CPU: <1 core, stable network

Required resources for the library are quite limited, as we only have a few loaded classes in memory, and the process is quite simple so CPU is not important as well. But we have a watcher for every worker on every node, and we keep the complete system topology on the master. So having 500-1000 workers should not be a problem, but 1 million might be in the first version of this library (further optimizations would not be complicated to handle that load if you plan to have so many workers).

We have the same requirements as for the [election library](https://github.com/gilles-degols/election). Only a few nodes can be configured to become master (and in charge of distributing the load), but you can have 100+ nodes acting as workers.

# Installation

As of now, we did not yet publish the library in Maven Central, so you need to build and publish it to your local repository, as well as its dependency, the election library. For example, once you pulled both projects:

`sbt +publishLocal`

In the sbt project:

```
lazy val clusterLibraryVersion = "0.0.1"
"net.degols" %% "cluster" % clusterLibraryVersion 
	exclude("log4j", "log4j") exclude("org.slf4j","slf4j-log4j12")
```

Another alternative, useful for contributors, is to have the "election" & "cluster" packages in the same directory as your "cluster-example" project using it. Then, by following this [build.sbt ](https://github.com/gilles-degols/cluster-example/blob/master/build.sbt)used for a cluster-example project, you can automatically build the cluster library alongside the election library and the cluster-example project itself.

# Usage

The best way to see how to use the library is to follow the code example provided [here](https://github.com/gilles-degols/cluster-example). After checking the code, familiarize yourself with the different steps describe below.

1. Set up your application.conf to indicate the default nodes taking part in the election process. You should only provide 3 or 5 nodes (so, with 2n+1 nodes in your system, you can accept the failure of n nodes). The elected master will be in charge of distributing the load for the entire system (which can have 100+ nodes)

   ```
   election.nodes = ["node1:2182", "node2:2182", "node3:2182"]
   election.akka.remote.netty.tcp.hostname = node1
   election.akka.remote.netty.tcp.port = 2182
   ```

   Note that your process might never be elected as master if the provided hostname & port is not part of the election nodes above.
2. In the same application.conf, you need to provide the configuration for the actor system used by the cluster library, as it will not re-use the same ports and thread pool as the one for the election process. You should use the same nodes as the one used for the election library (as the election needs to be collocated with the cluster management), but with a different port.

   ```
   cluster.nodes = ["node1:2190", "node2:2191", "node3:2192"]
   cluster.akka.remote.netty.tcp.hostname = node1
   cluster.akka.remote.netty.tcp.port = 2190
   ```
3. Create a [*LeaderExample *](https://github.com/gilles-degols/cluster-example/blob/master/app/net/degols/example/cluster/LeaderExample.scala)class extending the *WorkerLeader* class provided in the library, which describes the different capabilities of its own process, and knows how to start workers of multiple types. At boot, it will automatically find the current *Cluster Manager* of the system, and send its own capabilities. The manager will be in charge of checking those capabilities, verifying how many instances of each of them should be running, and send orders to launch the missing instances on various nodes (only the one able to run those classes).  \
   This *LeaderExample *must be a Singleton started automatically at boot.
4. Optional: By default we have a limited number of load balancers available in the library, but you can always define one yourself by following the example [here](https://github.com/gilles-degols/cluster-example/blob/master/app/net/degols/example/cluster/LeaderExample.scala). This load balancer must be available for every process that can be *Cluster Manager*.

Once you have everything set up, you do not need to care about any election process as the Cluster library will be in charge of handling the election, and in charge of sending the worker orders. You just need to create your own workers, and correctly configure the *LeaderExample* and that's it.