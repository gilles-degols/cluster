package net.degols.filesgate.libs.cluster.manager

import akka.actor.Actor
import com.google.inject.Inject

/**
  * Every worker of the cluster should extend this class to automatically contact the actor extending the ElectionActor
  * The id is the workerType
  */
abstract class WorkerActor @Inject()(workerLeader: WorkerLeader)(id: String) extends Actor {
  override def preStart(): Unit = {
    super.preStart()
  }
  override def receive: Receive = ???
}
