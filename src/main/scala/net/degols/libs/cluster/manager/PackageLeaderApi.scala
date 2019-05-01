package net.degols.libs.cluster.manager

import akka.actor.ActorRef

/**
  * Specify the different available W
  */
trait PackageLeaderApi {
  /**
    * Package name
    * @return
    */
  def packageName: String

  /**
    * We start the related actor
    * @param shortName
    */
  def startWorker(shortName: String, actorName: String): ActorRef

  /**
    * Automatically called by the related ClusterServiceLeader to put a reference to itself. It is useful to solve
    * circular dependencies
    */
  def setClusterServiceLeader(clusterServiceLeader: ClusterServiceLeader)

  /**
    * List of available WorkerActors given by the developer in the current jvm.
    */
  def workerInfos: List[WorkerInfo]
}
