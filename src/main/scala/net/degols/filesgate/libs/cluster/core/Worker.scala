package net.degols.filesgate.libs.cluster.core

import akka.actor.ActorRef
import org.joda.time.DateTime

/**
  * One instance of a WorkerType. Their number can grow up to any number, it will depend on the load of the system.
  */
class Worker(actorRef: ActorRef) {
  var lastPing: DateTime = new DateTime()

  // A Worker can be stopped if other WorkerTypes are not present
  var running: Boolean = false
}
