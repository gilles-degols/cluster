package net.degols.libs.cluster.core

import akka.actor.{ActorContext, ActorRef}
import net.degols.libs.cluster.Tools
import net.degols.libs.cluster.messages.{ClusterTopology, NodeInfo, StartWorkerActor, WorkerTypeInfo}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

/**
  * We could have multiple instances of WorkerManager running on the same machine. So the id is concatenation of
  * the hostname and a unique string gathered by the remote process (the PID for example). There is one WorkerManager
  * per JVM, it will handle multiple WorkerTypes.
  *
  * @param id
  */
class WorkerManager(val id: String, val actorRef: ActorRef) extends ClusterElement{
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  /**
    * All pipeline steps of the running system
    */
  private var _workerTypes: List[WorkerType] = List.empty[WorkerType]

  /**
    * Add a worker type we received from another JVM, if it does not exist yet
    * @param rawWorkerType
    */
  def addWorkerType(rawWorkerType: WorkerType): WorkerType = {
    _workerTypes.find(_ == rawWorkerType) match {
      case Some(previousWorkerType) => previousWorkerType
      case None =>
        _workerTypes = _workerTypes :+ rawWorkerType
        rawWorkerType
    }
  }

  /**
    * Ask the distant actor to launch a specific instance of given WorkerType. This communication is async but we create
    * a related Worker locally to remember that we just ask for it to launch
    */
  def startWorker(context: ActorContext, workerType: WorkerType, orderId: String): Unit = {
    workerTypes.find(_ == workerType) match {
      case None => logger.error(s"No WorkerType $workerType found in $this. Not possible to start worker.")
      case Some(w) =>
        val workerId = Worker.generateWorkerId(workerType.workerTypeInfo)
        Try {
          actorRef ! StartWorkerActor(context.self, workerType.workerTypeInfo, workerId, orderId)
        } match {
          case Success(res) =>
            // We still don't know if the actor is correctly started or not, but we must avoid creating a new one for the next soft distribution.
            val worker = Worker.fromWorkerIdAndActorRef(workerId, orderId, None)
            worker.setStatus(ClusterElementStarting())
            w.addWorker(worker)
          case Failure(err) =>
            logger.warn(s"Impossible to send a StartWorkerActor to $this for WorkerType: ${workerType}")
        }
    }
  }

  /**
    * Return the pipeline steps we have
    */
  def workerTypes: List[WorkerType] = _workerTypes

  /**
    * The port should normally be unique on the same node
    * @return
    */
  def port: String = Tools.remoteActorPath(actorRef).split("/")(2).split(":").last

  def canEqual(a: Any): Boolean = a.isInstanceOf[WorkerManager]
  override def equals(that: Any): Boolean =
    that match {
      case that: WorkerManager => that.canEqual(this) && this.hashCode == that.hashCode
      case _ => false
    }
  override def hashCode: Int = s"WorkerManager:$id".hashCode

  def reconstructFromClusterTopology(clusterTopology: ClusterTopology, currentNode: Node): Unit = {
    _workerTypes = List.empty[WorkerType]

    clusterTopology.workerActors.values.flatten.filter(workerActorHealth => {
      val rawNode = Node.fromNodeInfo(workerActorHealth.nodeInfo)
      // We only keep WorkerManagers for the current node
      rawNode == currentNode
    }).filter(workerActorHealth => {
      // We only keep the current WorkerManager
      val rawWorkerManager = new WorkerManager(workerActorHealth.jvmId, workerActorHealth.workerLeader)
      rawWorkerManager == this
    }).foreach(workerActorHealth => {
      val rawWorkerType = new WorkerType(workerActorHealth.workerTypeId, workerActorHealth.workerTypeInfo)
      addWorkerType(rawWorkerType)
    })

    workerTypes.foreach(workerType => workerType.reconstructFromClusterTopology(clusterTopology, currentNode, this))
  }
}

object WorkerManager {
  def fromWorkerTypeInfo(workerTypeInfo: WorkerTypeInfo): WorkerManager = {
    val workerManager = new WorkerManager(workerTypeInfo.jvmId, workerTypeInfo.actorRef) // WorkerTypeInfo sent by WorkerLeader, so the actor ref is ok
    workerManager
  }
}