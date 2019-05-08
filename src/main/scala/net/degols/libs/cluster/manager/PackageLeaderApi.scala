package net.degols.libs.cluster.manager

import akka.actor.{ActorContext, ActorRef}
import net.degols.libs.cluster.messages.UnknownWorker
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

/**
  * Specify the different available W
  */
trait PackageLeaderApi {
  private val logger = LoggerFactory.getLogger(getClass)

  /**
    * Package name
    * @return
    */
  val packageName: String

  /**
    * Set up directly by ClusterLeaderActor
    */
  protected var _context: ActorContext = _

  private[manager] def setContext(context: ActorContext): Unit = {
    _context = context
  }

  protected var _clusterServiceLeader: ClusterServiceLeader = _

  /**
    * List of available WorkerActors given by the developer in the current jvm, and the related way to start the
    * worker for a given WorkerInfo
    */
  protected val workers: mutable.Map[WorkerInfo, StartWorkerWrapper => ActorRef] = mutable.Map[WorkerInfo, StartWorkerWrapper => ActorRef]()

  /**
    * Store a new worker
    */
  protected def setWorker(workerInfo: WorkerInfo, fct: StartWorkerWrapper => ActorRef): Unit = {
    workers.put(workerInfo, fct)
  }

  /**
    * Small wrapper to avoid creating all workers in the initialization phase of the class
    */
  def setupWorkers(): Future[Unit]

  /**
    * Easy interface for the libraries using the PackageLeader
    * @return
    */
  def workerInfos: List[WorkerInfo] = workers.keys.toList

  /**
    * Easy interface for the libraries to start a worker
    * @param work
    * @return
    */
  def startWorker(work: StartWorkerWrapper): ActorRef = {
    logger.debug(s"Try to start the worker for the package $packageName and shortName: ${work.shortName}")
    workers.find(_._1.shortName == work.shortName) match {
      case Some(res) =>
        res._2(work)
      case None =>
        throw new UnknownWorker(s"Worker for shortName ${work.shortName} is not known in package $packageName.")
    }
  }

  /**
    * Automatically called by the related ClusterServiceLeader to put a reference to itself. It is useful to solve
    * circular dependencies.
    */
  private[manager] def setClusterServiceLeader(clusterServiceLeader: ClusterServiceLeader): Unit = {
    _clusterServiceLeader = clusterServiceLeader
  }



  /**
    * Method called once after the setup of _context and ClusterServiceLeader and once the manager is connected.
    * Could typically be used to send WorkerOrder
    */
  def postManagerConnection()(implicit ec: ExecutionContext): Future[Unit] = Future.successful{}
}
