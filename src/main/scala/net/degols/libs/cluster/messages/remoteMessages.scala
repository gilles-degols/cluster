package net.degols.libs.cluster.messages

import akka.actor.ActorRef
import com.typesafe.config.Config
import net.degols.libs.cluster.ClusterTools
import net.degols.libs.cluster.balancing.BasicLoadBalancerType
import net.degols.libs.cluster.core.Node
import net.degols.libs.election.{RemoteMessage, SimpleRemoteMessage}
import org.joda.time.DateTime
import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.json.{JsObject, Json}

import scala.util.Try

/**
  * Every RemoteMessage of this library should extend this class to easily manage them
  */
class ClusterRemoteMessage(actorRef: ActorRef) extends RemoteMessage(actorRef)

@SerialVersionUID(1L)
sealed trait InstanceType

@SerialVersionUID(1L)
case object JVMInstance extends InstanceType

@SerialVersionUID(1L)
case object ClusterInstance extends InstanceType

/**
  * Message sent by any JVM asking the Manager to start specific instances of some WorkerTypeId on any JVM.
  * An update of the WorkerTypeInfo can be sent at any moment by a JVM. To be able to differentiate an update versus some
  * new information we use the "id" sent by the JVM (so it's up to the developer to correctly select the "id" field).
  * This also allows to have multiple JVMs sending the same order but only being executed once (this will happen frequently)
  * @param actorRef
  * @param workerTypeId
  * @param loadBalancerType The load balancer in charge of handling the number of actors we want to be started. If
  *                         two different orders are sent with the same id, only the first received will be used.
  * @param id Like a sub-group for a given WorkerTypeId (see explanation above)
  * @param metadata any specific information about the WorkerTypeInfo. Useful to create homemade load balancer using
  *                 those metadata information to correctly distribute the load. For example, for Filesgate, it's useful
  *                 to know what kind of PipelineStep we have.
  *                 Stringified json
  */
@SerialVersionUID(1L)
case class WorkerTypeOrder(actorRef: ActorRef, workerTypeId: String, loadBalancerType: LoadBalancerType, id: String, metadata: String) extends ClusterRemoteMessage(actorRef){
  /**
    * Automatically added by the WorkerLeader when it sends its info.
    * This does not means this WorkerTypeInfo can have the related actor started on the nodeInfo
    */
  var nodeInfo: NodeInfo = _

  override val toString: String = s"WorkerTypeOrder ($id): $workerTypeId / ${ClusterTools.remoteActorPath(actorRef)} @ $creationDatetime"
}

/**
  * Message sent when the system is booting up. The WorkerLeader sent multiple WorkerTypeInfo indicating which actors
  * it is able to start, but it does not start any actor directly, the WorkerTypeOrder still needs to be sent with
  * the appropriate WorkerTypeOrder.
  * You need to provide the appropriate LoadBalancer information
  * @param actorRef the one from the current WorkerLeader
  * @param workerTypeId unique id of the WorkerActor to differentiate them
  * @param metadata stringified json
  */
@SerialVersionUID(1L)
case class WorkerTypeInfo(actorRef: ActorRef, workerTypeId: String, metadata: String) extends ClusterRemoteMessage(actorRef){
  /**
    * Automatically added by the WorkerLeader when it sends its info.
    * This does not means this WorkerTypeInfo can have the related actor started on the nodeInfo
    */
  var nodeInfo: NodeInfo = _

  override val toString: String = s"WorkerTypeInfo: $workerTypeId / ${ClusterTools.remoteActorPath(actorRef)} @ $creationDatetime"
}

/**
  * To avoid strange problems, you might need to load a proper worker info with the related actor ref. The nodeInfo
  * must come from the appropriate node (most likely different than the one in the given WorkerTypeInfo), be careful
  */
object WorkerTypeInfo {
  def fromWorkerTypeInfo(workerTypeInfo: WorkerTypeInfo, actorRef: ActorRef, nodeInfo: NodeInfo): WorkerTypeInfo = {
    val newWorkerTypeInfo = WorkerTypeInfo(actorRef, workerTypeInfo.workerTypeId, workerTypeInfo.metadata)
    newWorkerTypeInfo.nodeInfo = nodeInfo
    newWorkerTypeInfo
  }
}

/**
  * Some trait used to indicate we ask information from the Manager
  */
trait ClusterInfo

/**
  * Ask some information about a given ActorRef. The manager will reply with an "Option[InfoFromActorRef]"
  */
@SerialVersionUID(0L)
case class GetInfoFromActorRef(actorRef: ActorRef, targetActorRef: ActorRef) extends ClusterRemoteMessage(actorRef) with ClusterInfo

@SerialVersionUID(0L)
case class InfoFromActorRef(actorRef: ActorRef, targetActorRef: ActorRef, workerTypeId: String, workerTypeOrder: WorkerTypeOrder) extends ClusterRemoteMessage(actorRef) with ClusterInfo

/**
  * A worker might want to contact other workers. To avoid sending the entire system topology to each worker, when needed,
  * the worker contacts the Manager to know which actors are available for a given WorkerType.
  * This system is not meant to be used for every message, otherwise the manager will be overloaded. A Cache system
  * must be added in the Worker itself.
  * If an OrderId is specified, only return the ActorRefs for the given workerTypeId and orderId
  * If an isRunning is specifed, only return the ActorRefs if the related worker is set as 'Running'
  * The return will be an Seq[ActorRef]
  */
@SerialVersionUID(1L)
case class GetActorRefsFor(actorRef: ActorRef, workerTypeId: String, orderId: Option[String], isRunning: Option[Boolean]) extends ClusterRemoteMessage(actorRef) with ClusterInfo

/**
  * Return a list of all the different WorkerTypeId existing in the system.
  *
  * The return will be a Seq[String]
  */
@SerialVersionUID(1L)
case class GetAllWorkerTypeIds(actorRef: ActorRef) extends ClusterRemoteMessage(actorRef) with ClusterInfo


/**
  * Order from a Manager to a WorkerLeader to start an instance of a WorkerActor
  * @param actorRef
  */
@SerialVersionUID(1L)
case class StartWorkerActor(actorRef: ActorRef, workerTypeInfo: WorkerTypeInfo, workerTypeOrder: WorkerTypeOrder, workerId: String) extends ClusterRemoteMessage(actorRef){
  override def toString: String = s"StartWorkerActor: ${workerTypeInfo.workerTypeId} / $actorRef / $workerId / ${workerTypeOrder.id} @ $creationDatetime"
}

/**
  * Result of a started WorkerActor
  * @param actorRef
  */
@SerialVersionUID(1L)
case class StartedWorkerActor(actorRef: ActorRef, startWorkerActor: StartWorkerActor, runningActorRef: ActorRef) extends ClusterRemoteMessage(actorRef){
  var nodeInfo: NodeInfo = _
  override def toString: String = s"StartedWorkerActor: ${startWorkerActor.workerId} - ${startWorkerActor.workerTypeInfo.workerTypeId} / ${ClusterTools.remoteActorPath(runningActorRef)} @ $creationDatetime"
}

/**
  * When we failed to start a WorkerActor, we try to return an appropriate message (even if we should also handle
  * the fact that we could also never receive any message back)
  * @param actorRef
  * @param startWorkerActor
  */
@SerialVersionUID(1L)
case class FailedWorkerActor(actorRef: ActorRef, startWorkerActor: StartWorkerActor, exception: Exception) extends ClusterRemoteMessage(actorRef){
  var nodeInfo: NodeInfo = _
  override def toString: String = s"FailedWorkerActor: ${startWorkerActor.workerTypeInfo.workerTypeId} / ${ClusterTools.remoteActorPath(actorRef)} @ $creationDatetime"
}

/**
  * In some specific cases, the Manager can decide to stop WorkerActor on some nodes, and start new ones on other nodes.
  * This messages asks the actor to stop completely, and it's directly received by the WorkerActor.
  * @param actorRef
  */
@SerialVersionUID(1L)
case class KillWorkerActor(actorRef: ActorRef) extends ClusterRemoteMessage(actorRef){
  override def toString: String = s"KillWorkerActor: ${ClusterTools.remoteActorPath(actorRef)} @ $creationDatetime"
}

/**
  * In some specific cases, the Manager might just want to "pause" an actor temporarily instead of killing it completely.
  * This message is directly received by the WorkerActor.
  * @param actorRef
  * @param timeInMilliSeconds number of milliseconds we need to wait for the pause before continuing work. If no given value,
  *                           we wait for a restart from the Manager.
  * @param timeSinceCreation should we wait for timeInMilliseconds since the creation time of the message (true) or since
  *                          the handling of the message (false). Default is false
  */
@SerialVersionUID(1L)
case class PauseWorkerActor(actorRef: ActorRef, timeInMilliSeconds: Option[Long] = None, timeSinceCreation: Boolean = false) extends ClusterRemoteMessage(actorRef){
  // Contains the handling time of the message. Useful to know when the pause started
  var handlingTime: Option[Long] = None

  def isInPause: Boolean = {
    timeInMilliSeconds match {
      case None => true // No end, unless a ResumeWorkerActor appears
      case Some(breakTime) =>
        val startTime: Long = if(timeSinceCreation) creationDatetime.getMillis
        else if(handlingTime.isDefined) handlingTime.get
        else ClusterTools.datetime().getMillis // Message not yet handled, it means we are not yet in pause

        startTime + breakTime < ClusterTools.datetime().getMillis
    }
  }
  override def toString: String = s"PauseWorkerActor: ${ClusterTools.remoteActorPath(actorRef)} @ $creationDatetime during ${timeInMilliSeconds}"
}

/**
  * Resume a paused workerActor
  * @param actorRef
  */
@SerialVersionUID(1L)
case class ResumeWorkerActor(actorRef: ActorRef) extends ClusterRemoteMessage(actorRef){
  override def toString: String = s"ResumeWorkerActor: ${ClusterTools.remoteActorPath(actorRef)} @ $creationDatetime"
}

/**
  * To avoid handling some very specific race conditions when a Manager dies just after it sent a StartWorkerActor, the
  * Manager can request information about all actors of the current WorkerLeader to reconstruct its topology.
  */
@SerialVersionUID(1L)
case class RequestWorkerActorTopology(actorRef: ActorRef) extends ClusterRemoteMessage(actorRef) {
  override def toString: String = s"RequestWorkerActorTopology: ${ClusterTools.remoteActorPath(actorRef)} @ $creationDatetime"
}

/**
  * Abstract class used for jvm and cluster topology
  */
@SerialVersionUID(1L)
abstract class WorkerActorTopology(actorRef: ActorRef) extends ClusterRemoteMessage(actorRef){
  // List of running workers (workerTypeId -> workerActorHealth).
  var workerActors: Map[String, Seq[WorkerActorHealth]] = Map.empty[String, Seq[WorkerActorHealth]]

  def prettyDisplay: String = {
    workerActors.map(workerInfo => {
      val runningActors = workerInfo._2.count(_.isRunning)
      val stoppedActors = workerInfo._2.length - runningActors
      val stoppedActorsMessage = if(runningActors > 0) s"${stoppedActors} stopped actors"
      s"${workerInfo._1} -> $runningActors running actors $stoppedActorsMessage"
    }).mkString("\n")
  }

  def getWorkerActors(workerTypeId: String): Seq[WorkerActorHealth] = workerActors.getOrElse(workerTypeId, List.empty[WorkerActorHealth])

  def addWorkerActor(workerActorHealth: WorkerActorHealth): Unit = {
    val workerList: Seq[WorkerActorHealth] = workerActors.get(workerActorHealth.workerTypeId) match {
      case Some(workers) => workers.filterNot(_.workerActorRef == workerActorHealth.workerActorRef) :+ workerActorHealth // Filter the same WorkerActorHealth as the ClusterTopology update its information that way
      case None => List(workerActorHealth)
    }
    workerActors = workerActors ++ Map(workerActorHealth.workerTypeId -> workerList)
  }

  /**
    * Return true if a workerActor has been removed or not
    * @param actorRef
    * @return
    */
  def removeWorkerActor(actorRef: ActorRef): Boolean = {
    val count = workerActors.values.flatten.size
    workerActors = workerActors.map(workerInfo => {
      workerInfo._1 -> workerInfo._2.filterNot(_.workerActorRef == actorRef)
    })
    count > workerActors.values.flatten.size
  }

  override def toString: String = s"Topology: $actorRef @ $creationDatetime"
}

/**
  * Class representing the current JVM Topology. This is specific to a jvm and it is normally only shared with the Manager
  */
@SerialVersionUID(1L)
case class JVMTopology(actorRef: ActorRef) extends WorkerActorTopology(actorRef) {
  override def toString: String = s"JVMTopology: $actorRef @ $creationDatetime"
}

/**
  * Contain various information about a WorkerActor
  */
@SerialVersionUID(1L)
case class WorkerActorHealth(actorRef: ActorRef, workerTypeInfo: WorkerTypeInfo, workerTypeOrder: WorkerTypeOrder, workerActorRef: ActorRef, nodeInfo: NodeInfo, workerLeader: ActorRef, workerActorId: String) extends ClusterRemoteMessage(actorRef) {
  def workerTypeId: String = workerTypeInfo.workerTypeId

  private var _clusterRemoteMessages: Seq[ClusterRemoteMessage] = List.empty[ClusterRemoteMessage]

  /**
    * Total number of processed messages (without ClusterRemoteMessage)
    */
  private var _processedMessages: Long = 0L

  /**
    * Last processed message with success (no ClusterRemoteMessage!)
    */
  private var _lastSuccessfulMessage: Option[(DateTime, Any)] = None

  /**
    * Last processed message with failure (no ClusterRemoteMessage!)
    */
  private var _lastFailedMessage: Option[(DateTime, Any)] = None

  def setLastSuccessfulMessage(message: Any): Unit = _lastSuccessfulMessage = Option(ClusterTools.datetime(), message)

  def setLastFailedMessage(message: Any): Unit = _lastFailedMessage = Option(ClusterTools.datetime(), message)

  def incrementProcessedMessages(value: Long = 0): Unit = _processedMessages += value

  def processedMessages: Long = _processedMessages

  def addClusterRemoteMessage(message: ClusterRemoteMessage): Unit = {
    _clusterRemoteMessages = _clusterRemoteMessages :+ message
    if(_clusterRemoteMessages.size > 30) { // We remember between 20 & 30 messages maximum. Normally they are processed if stored here, so we can clean them without any worry
      _clusterRemoteMessages = _clusterRemoteMessages.drop(10)
    }
  }

  def isRunning: Boolean = {
    // An actor directly starts after its creation, no need for a custom ClusterRemoteMessage for that.
    // But it might be stopped.
    var running = true
    var isKilled = false
    _clusterRemoteMessages.foreach {
      case x: KillWorkerActor => isKilled = true
      case x: PauseWorkerActor => running = running && !x.isInPause // We could have two consecutive pauses, so it's best to verify the previous value of running
      case x: ResumeWorkerActor => running = true
      case x => // Other message we don't care about
    }
    !isKilled && running
  }
  override def toString: String = s"WorkerActorHealth: $actorRef @ $creationDatetime"
}

/**
  * Class representing the entire cluster. This is shared with every jvm every x seconds if the topology changed during those
  * x seconds. The message is sent to the WorkerLeader, which is in charge of storing it.
  * TODO: Reduce the size of the ClusterTopology as it contains the WorkerActorHealth. We should only send a subset to every
  * jvm (based on what they ask for, by default, everything), not the entire object
  */
@SerialVersionUID(1L)
case class ClusterTopology(actorRef: ActorRef) extends WorkerActorTopology(actorRef) {
  override def toString: String = s"ClusterTopology: $actorRef @ $creationDatetime"
}

/**
  * Handle the load balancing configuration. Custom implementations can be done by the developers when needed. The class
  * must be reachable by any JVM. The message needs to be serializable, but the implementation does not need to be necessarily.
  */
@SerialVersionUID(1L)
trait LoadBalancerType extends SimpleRemoteMessage

/**
  * Load the appropriate LoadBalancer from a configuration
  */
object LoadBalancerType {
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  def loadFromConfig(config: Config): LoadBalancerType = {
    val loadBalancer = getLoadBalancerType(config).getOrElse(BasicLoadBalancerType.CONFIGURATION_KEY)

    if(loadBalancer == BasicLoadBalancerType.CONFIGURATION_KEY) {
      BasicLoadBalancerType.loadFromConfig(config)
    } else {
      throw new Exception("No valid configuration key found for 'load-balancer', you should load it yourselves as the given load-balancer is not in the cluster library.")
    }
  }

  def getLoadBalancerType(config: Config): Option[String] = {
    Try{config.getString("load-balancer")}.toOption
  }
}

/**
  * Small information about the current node. Multiple JVMs could run on the same nodes.
  */
@SerialVersionUID(1L)
case class NodeInfo(networkHostname: String, localHostname: String)

