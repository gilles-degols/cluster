package net.degols.filesgate.libs.cluster.messages

import akka.actor.ActorRef
import net.degols.filesgate.libs.cluster.Tools
import net.degols.filesgate.libs.cluster.core.Node
import net.degols.filesgate.libs.election.{RemoteMessage, SimpleRemoteMessage}
import org.joda.time.DateTime

/**
  * Every RemoteMessage of this library should extend this class to easily manage them
  */
class ClusterRemoteMessage(actorRef: ActorRef) extends RemoteMessage(actorRef)

@SerialVersionUID(10010L)
sealed trait InstanceType

@SerialVersionUID(10020L)
case object JVMInstance extends InstanceType

@SerialVersionUID(10030L)
case object ClusterInstance extends InstanceType

/**
  * Message sent when the system is booting up. The WorkerLeader sent multiple WorkerTypeInfo indicating which actors
  * it is able to start. You need to provide the appropriate LoadBalancer information
  * @param actorRef the one from the current WorkerLeader
  * @param workerTypeId unique id of the WorkerActor to differentiate them
  */
@SerialVersionUID(10040L)
case class WorkerTypeInfo(actorRef: ActorRef, workerTypeId: String, loadBalancerType: LoadBalancerType) extends ClusterRemoteMessage(actorRef){
  // Automatically added by the WorkerLeader when it sends its info
  var nodeInfo: NodeInfo = _

  override val toString: String = s"WorkerTypeInfo: $workerTypeId / ${Tools.remoteActorPath(actorRef)} @ $creationDatetime"
}

/**
  * To avoid strange problems, you might need to load a proper worker info with the related actor ref. The nodeInfo
  * must come from the appropriate node (most likely different than the one in the given WorkerTypeInfo), be careful
  */
object WorkerTypeInfo {
  def fromWorkerTypeInfo(workerTypeInfo: WorkerTypeInfo, actorRef: ActorRef, nodeInfo: NodeInfo): WorkerTypeInfo = {
    val newWorkerTypeInfo = WorkerTypeInfo(actorRef, workerTypeInfo.workerTypeId, workerTypeInfo.loadBalancerType)
    newWorkerTypeInfo.nodeInfo = nodeInfo
    newWorkerTypeInfo
  }
}

/**
  * Order from a Manager to a WorkerLeader to start an instance of a WorkerActor
  * @param actorRef
  */
@SerialVersionUID(10050L)
case class StartWorkerActor(actorRef: ActorRef, workerTypeInfo: WorkerTypeInfo, workerId: String) extends ClusterRemoteMessage(actorRef){
  override def toString: String = s"StartWorkerActor: ${workerTypeInfo.workerTypeId} / $actorRef / $workerId @ $creationDatetime"
}

/**
  * Result of a started WorkerActor
  * @param actorRef
  */
@SerialVersionUID(10060L)
case class StartedWorkerActor(actorRef: ActorRef, startWorkerActor: StartWorkerActor, runningActorRef: ActorRef) extends ClusterRemoteMessage(actorRef){
  var nodeInfo: NodeInfo = _
  override def toString: String = s"StartedWorkerActor: ${startWorkerActor.workerId} - ${startWorkerActor.workerTypeInfo.workerTypeId} / ${Tools.remoteActorPath(runningActorRef)} @ $creationDatetime"
}

/**
  * When we failed to start a WorkerActor, we try to return an appropriate message (even if we should also handle
  * the fact that we could also never receive any message back)
  * @param actorRef
  * @param startWorkerActor
  */
@SerialVersionUID(10070L)
case class FailedWorkerActor(actorRef: ActorRef, startWorkerActor: StartWorkerActor, exception: Exception) extends ClusterRemoteMessage(actorRef){
  var nodeInfo: NodeInfo = _
  override def toString: String = s"FailedWorkerActor: ${startWorkerActor.workerTypeInfo.workerTypeId} / ${Tools.remoteActorPath(actorRef)} @ $creationDatetime"
}

/**
  * In some specific cases, the Manager can decide to stop WorkerActor on some nodes, and start new ones on other nodes.
  * This messages asks the actor to stop completely, and it's directly received by the WorkerActor.
  * @param actorRef
  */
@SerialVersionUID(10080L)
case class KillWorkerActor(actorRef: ActorRef) extends ClusterRemoteMessage(actorRef){
  override def toString: String = s"KillWorkerActor: ${Tools.remoteActorPath(actorRef)} @ $creationDatetime"
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
@SerialVersionUID(10090L)
case class PauseWorkerActor(actorRef: ActorRef, timeInMilliSeconds: Option[Long] = None, timeSinceCreation: Boolean = false) extends ClusterRemoteMessage(actorRef){
  // Contains the handling time of the message. Useful to know when the pause started
  var handlingTime: Option[Long] = None

  def isInPause: Boolean = {
    timeInMilliSeconds match {
      case None => true // No end, unless a ResumeWorkerActor appears
      case Some(breakTime) =>
        val startTime: Long = if(timeSinceCreation) creationDatetime.getMillis
        else if(handlingTime.isDefined) handlingTime.get
        else Tools.datetime().getMillis // Message not yet handled, it means we are not yet in pause

        startTime + breakTime < Tools.datetime().getMillis
    }
  }
  override def toString: String = s"PauseWorkerActor: ${Tools.remoteActorPath(actorRef)} @ $creationDatetime during ${timeInMilliSeconds}"
}

/**
  * Resume a paused workerActor
  * @param actorRef
  */
@SerialVersionUID(10100L)
case class ResumeWorkerActor(actorRef: ActorRef) extends ClusterRemoteMessage(actorRef){
  override def toString: String = s"ResumeWorkerActor: ${Tools.remoteActorPath(actorRef)} @ $creationDatetime"
}

/**
  * To avoid handling some very specific race conditions when a Manager dies just after it sent a StartWorkerActor, the
  * Manager can request information about all actors of the current WorkerLeader to reconstruct its topology.
  */
@SerialVersionUID(10110L)
case class RequestWorkerActorTopology(actorRef: ActorRef) extends ClusterRemoteMessage(actorRef) {
  override def toString: String = s"RequestWorkerActorTopology: ${Tools.remoteActorPath(actorRef)} @ $creationDatetime"
}

/**
  * Abstract class used for jvm and cluster topology
  */
@SerialVersionUID(10180L)
abstract class WorkerActorTopology(actorRef: ActorRef) extends ClusterRemoteMessage(actorRef){
  // List of running workers (workerTypeId -> workerActorHealth).
  var workerActors: Map[String, List[WorkerActorHealth]] = Map.empty[String, List[WorkerActorHealth]]

  def prettyDisplay: String = {
    workerActors.map(workerInfo => {
      val runningActors = workerInfo._2.count(_.isRunning)
      val stoppedActors = workerInfo._2.length - runningActors
      val stoppedActorsMessage = if(runningActors > 0) s"${stoppedActors} stopped actors"
      s"${workerInfo._1} -> $runningActors running actors $stoppedActorsMessage"
    }).mkString("\n")
  }

  def getWorkerActors(workerTypeId: String): List[WorkerActorHealth] = workerActors.getOrElse(workerTypeId, List.empty[WorkerActorHealth])

  def addWorkerActor(workerActorHealth: WorkerActorHealth): Unit = {
    val workerList: List[WorkerActorHealth] = workerActors.get(workerActorHealth.workerTypeId) match {
      case Some(workers) => workers :+ workerActorHealth
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
@SerialVersionUID(10120L)
case class JVMTopology(actorRef: ActorRef) extends WorkerActorTopology(actorRef) {
  override def toString: String = s"JVMTopology: $actorRef @ $creationDatetime"
}

/**
  * Contain various information about a WorkerActor
  */
@SerialVersionUID(10130L)
case class WorkerActorHealth(actorRef: ActorRef, workerTypeInfo: WorkerTypeInfo, workerActorRef: ActorRef, nodeInfo: NodeInfo, workerLeader: ActorRef, workerActorId: String) extends ClusterRemoteMessage(actorRef) {
  def workerTypeId: String = workerTypeInfo.workerTypeId

  private var _clusterRemoteMessages: List[ClusterRemoteMessage] = List.empty[ClusterRemoteMessage]

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

  def setLastSuccessfulMessage(message: Any): Unit = _lastSuccessfulMessage = Option(Tools.datetime(), message)

  def setLastFailedMessage(message: Any): Unit = _lastFailedMessage = Option(Tools.datetime(), message)

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
  */
@SerialVersionUID(10140L)
case class ClusterTopology(actorRef: ActorRef) extends WorkerActorTopology(actorRef) {
  override def toString: String = s"ClusterTopology: $actorRef @ $creationDatetime"
}

/**
  * Handle the load balancing configuration. Custom implementations can be done by the developers when needed. The class
  * must be reachable by any JVM. The message needs to be serializable, but the implementation does not need to be necessarily.
  */
@SerialVersionUID(10150L)
trait LoadBalancerType extends SimpleRemoteMessage{}

/**
  * Basic load balancing: start the number of asked instances, no more, no less.
  * @param instances
  * @param instanceType
  */
@SerialVersionUID(10160L)
case class BasicLoadBalancerType(instances: Int, instanceType: InstanceType = ClusterInstance) extends LoadBalancerType {
  override def toString: String = {
    val location = if(instanceType == JVMInstance) "jvm" else "cluster"
    s"BasicLoadBalancer: $instances instances/$location"
  }
}

/**
  * Load balancing where we want to use multiple ips (for example, we have multiple servers and each server has 1 or more
  * ips they can use to access websites without being banned)
  * @param ips
  * @param instanceType
  */
@SerialVersionUID(10170L)
case class IPLoadBalancerType(ips: List[String], instanceType: InstanceType = ClusterInstance) extends LoadBalancerType

/**
  * Small information about the current node. Multiple JVMs could run on the same nodes.
  */
@SerialVersionUID(10180L)
case class NodeInfo(networkHostname: String, localHostname: String)

