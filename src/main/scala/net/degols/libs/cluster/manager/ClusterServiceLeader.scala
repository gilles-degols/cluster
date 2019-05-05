package net.degols.libs.cluster.manager

import akka.actor.{ActorContext, ActorRef, Cancellable}
import akka.util.Timeout
import javax.inject.{Inject, Singleton}
import net.degols.libs.cluster.ClusterTools
import net.degols.libs.cluster.messages.{ClusterRemoteMessage, ClusterTopology, FailedWorkerActor, JVMTopology, NodeInfo, StartWorkerActor, StartedWorkerActor, WorkerActorHealth, WorkerTypeInfo, WorkerTypeOrder}
import org.slf4j.LoggerFactory
import akka.pattern.ask
import play.api.libs.json.{JsObject, Json}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future, TimeoutException}
import scala.util.{Failure, Random, Success, Try}

/**
  * Local object used to communicate with the developer, asking him to start a specific worker
  * @param infoMetadata Information that can be sent when we indicate the different WorkerInfo we have in a package
  * @param orderMetadata Information that can be sent when we order a specific worker to start
  * @param initialMessage The Initial message received by the cluster library. Might be useful in some cases
  */
case class StartWorkerWrapper(shortName: String, actorName: String, infoMetadata: JsObject, orderMetadata: JsObject, initialMessage: StartWorkerActor)

/**
  * Contain tools to communicate with the Manager of the cluster. Typically useful to send WorkerOrder
  */
@Singleton
class ClusterServiceLeader {
  private val logger = LoggerFactory.getLogger(getClass)

  /**
    * Tool to easily communicate with other JVMs
    */
  final val communication: Communication = new Communication(this)

  /**
    * Node information is set by the ClusterLeaderActor at start-up. In very specific cases, the developer might want
    * to override it (99.99% you should not do it)
    */
  var nodeInfo: Option[NodeInfo] = None


  /**
    * Current Manager in charge of the cluster (it can be different than the localManager obviously)
    */
  var manager: Option[ActorRef] = None

  /**
    * Number of started Actors locally, to be sure to not have any conflict in their name
    */
  var startedWorkers: Long = 0L

  /**
    * Topology of the cluster. Must remain private as in the future it might disappear (to avoid sending the complete
    * topology at each call we could go through the manager to have the needed actorRefs)
    */
  private[manager] var _clusterTopology: Option[ClusterTopology] = None

  /**
    * Information about the current topology we have in this jvm. Set up by the ClusterLeaderActor
    */
  private[manager] var jvmTopology: JVMTopology = _


  /**
    * Convert a local WorkerInfo to a WorkerTypeInfo to send it to the Manager
    */
  private def convertWorkerInfo(componentName: String, packageName: String, workerInfo: WorkerInfo)(implicit context: ActorContext): WorkerTypeInfo = {
    //WorkerTypeOrder(context.self, workerOrder.fullName, workerOrder.balancerType, orderId, workerOrder.metadata)
    val fullName = Communication.fullActorName(componentName, packageName, workerInfo.shortName)
    WorkerTypeInfo(context.self, fullName, workerInfo.metadata.toString())
  }

  /**
    * Optionally return a WorkerOrder based on a WorkerInfo if it already contains a balancer
    */
  private def loadWorkerOrderFromInfo(componentName: String, packageName: String, workerInfo: WorkerInfo)(implicit context: ActorContext): Option[WorkerOrder] = {
    workerInfo.balancerType
      .map(balancer => {
        val fullName = Communication.fullActorName(componentName, packageName, workerInfo.shortName)
        WorkerOrder(fullName, balancer)
      })
  }

  /**
    * Send all WorkerInfo to the Manager
    */
  private[manager] def notifyWorkerTypeInfo(componentLeaderApi: ComponentLeaderApi)(implicit context: ActorContext): Unit = {
    manager match {
      case Some(currentManager) =>
        logger.debug(s"Send all workerTypeInfo to the manager $currentManager")
        componentLeaderApi.packageLeaders.foreach(packageLeader => {
          packageLeader.workerInfos.foreach(workerInfo => {
            // First send the workerTypeInfo
            val workerTypeInfo = convertWorkerInfo(componentLeaderApi.componentName, packageLeader.packageName, workerInfo)
            workerTypeInfo.nodeInfo = nodeInfo.get
            currentManager ! workerTypeInfo

            // If there is balancer, we can directly send the WorkerOrder to the manager
            loadWorkerOrderFromInfo(componentLeaderApi.componentName, packageLeader.packageName, workerInfo)
              .foreach(order => {
                currentManager ! communication.convertWorkerOrder(order)
              })
          })
        })

      case None => // Nothing to do
        logger.error("Not possible to notify the Manager about our WorkerInfo as none is found...")
    }
  }

  /**
    * Handle some cluster messages, so nothing related to the election system in itself;
    */
  private[manager] def handleClusterRemoteMessage(componentLeaderApi: ComponentLeaderApi, clusterRemoteMessage: ClusterRemoteMessage)(implicit context: ActorContext) = Try {
    // The entire method is surrounded by a Try to be sure we don't crash for any reason. But we should handle every
    // message correctly by default
    clusterRemoteMessage match {
      case message: ClusterTopology =>
        logger.info(s"[WorkerLeader] Received ClusterTopology: $message")
        _clusterTopology = Option(message)
      case message: StartWorkerActor =>
        handleStartWorker(componentLeaderApi, message)
      case x =>
        logger.error(s"Unknown ClusterRemoteMessage received $x, this should never happen!")
    }
  }

  /**
    * In charge of handling a StartWorkerActor message received from the manager, and try to start it
    * @param message
    */
  private def handleStartWorker(componentLeaderApi: ComponentLeaderApi, message: StartWorkerActor)(implicit context: ActorContext): Unit = {
    // The worker name is not mandatory, it's just to avoid having the developer deals with it if it does not need to
    logger.info(s"Starting worker type id: ${message.workerTypeInfo.workerTypeId}")

    // Find the related packageLeader who can start the worker
    val packageInCharge: Option[PackageLeaderApi] = componentLeaderApi.packageLeaders.find(packageLeader => {
      packageLeader.workerInfos.exists(workerInfo => {
        val fullName = Communication.fullActorName(componentLeaderApi.componentName, packageLeader.packageName, workerInfo.shortName)
        fullName == message.workerTypeInfo.workerTypeId
      })
    })

    packageInCharge match {
      case Some(p) =>
        startWorker(p, message)
      case None =>
        logger.error(s"No PackageLeader available to start the workerTypeId ${message.workerTypeInfo.workerTypeId}")
    }
  }


  private def startWorker(packageLeaderApi: PackageLeaderApi, message: StartWorkerActor)(implicit context: ActorContext): Unit = {
    val initialName = message.workerTypeInfo.workerTypeId.split(":").drop(2).mkString(":")
    val workerName = s"${message.workerTypeInfo.workerTypeId}-$startedWorkers"
    startedWorkers += 1

    val wrapper = StartWorkerWrapper(initialName, workerName, Json.parse(message.workerTypeInfo.metadata).as[JsObject], Json.parse(message.workerTypeOrder.metadata).as[JsObject], message)
    Try{packageLeaderApi.startWorker(wrapper)} match {
      case Success(res) =>
        // Setting a watcher can lead to failure if the actors just die at that moment
        Try{context.watch(res)} match {
          case Success(s) =>
            val workerActorHealth = WorkerActorHealth(context.self, message.workerTypeInfo, message.workerTypeOrder, res, nodeInfo.get, context.self, message.workerId)
            jvmTopology.addWorkerActor(workerActorHealth)
            val m = StartedWorkerActor(context.self, message, res)
            m.nodeInfo = nodeInfo.get
            message.actorRef ! m
            message.actorRef ! workerActorHealth
          case Failure(e) =>
            logger.error(s"Impossible to set a watcher, the actor probably died mid-way: $e")
            val m = FailedWorkerActor(context.self, message, new Exception("Failing actor after starting it"))
            m.nodeInfo = nodeInfo.get
            message.actorRef ! m
        }
      case Failure(err) =>
        val excep = err match {
          case exception: Exception => exception
          case _ => new Exception(s"Unknown error while starting a workerActor: $err")
        }
        logger.error(s"Got an exception while trying to start a worker ${initialName}: ${ClusterTools.formatStacktrace(excep)}")
        val m = FailedWorkerActor(context.self, message, excep)
        m.nodeInfo = nodeInfo.get
        message.actorRef ! m
    }
  }
}