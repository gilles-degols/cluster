package net.degols.libs.cluster.manager

import akka.actor.{ActorContext, ActorRef}

/**
  * Specify the different available W
  */
trait PackageLeaderApi {
  /**
    * Package name
    * @return
    */
  val packageName: String

  /**
    * We start the related actor
    */
  def startWorker(work: StartWorkerWrapper): ActorRef


  /**
    * Set up directly by ClusterLeaderActor
    */
  protected var _context: ActorContext = _

  private[manager] def setContext(context: ActorContext): Unit = {
    _context = context
  }

  protected var _clusterServiceLeader: ClusterServiceLeader = _

  /**
    * Automatically called by the related ClusterServiceLeader to put a reference to itself. It is useful to solve
    * circular dependencies.
    */
  private[manager] def setClusterServiceLeader(clusterServiceLeader: ClusterServiceLeader): Unit = {
    _clusterServiceLeader = clusterServiceLeader
  }

  /**
    * List of available WorkerActors given by the developer in the current jvm.
    */
  def workerInfos: Seq[WorkerInfo]

  /**
    * Method called once after the setup of _context and ClusterServiceLeader and once the manager is connected.
    * Could typically be used to send WorkerOrder
    */
  def postManagerConnection(): Unit = Unit
}
