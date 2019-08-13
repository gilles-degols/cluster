package net.degols.libs.cluster.manager

import java.util.concurrent.TimeUnit

import akka.actor.{ActorContext, ActorRef, Cancellable}
import akka.util.Timeout
import javax.inject.{Inject, Singleton}
import net.degols.libs.cluster.ClusterTools
import net.degols.libs.cluster.messages.{ClusterInfo, ClusterRemoteMessage, ClusterTopology, FailedWorkerActor, JVMTopology, MissingManager, MissingPackageLeader, NodeInfo, StartWorkerActor, StartedWorkerActor, WorkerActorHealth, WorkerTypeInfo, WorkerTypeOrder}
import org.slf4j.LoggerFactory
import akka.pattern.ask
import com.github.benmanes.caffeine.cache.Caffeine
import net.degols.libs.cluster.configuration.{ClusterConfiguration, ClusterConfigurationApi, DefaultClusterConfiguration}
import play.api.libs.json.{JsObject, Json}
import scalacache.caffeine.CaffeineCache
import scalacache._
import scalacache.caffeine._
import scalacache.modes.scalaFuture._
import scalacache.Entry

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
  * Contain tools to communicate with the Manager of the cluster. Typically useful to send WorkerOrders
  */
@Singleton
class ClusterServiceLeader @Inject()(clusterConfigurationApi: ClusterConfigurationApi) {
  private val logger = LoggerFactory.getLogger(getClass)
  implicit val ec: ExecutionContext = clusterConfigurationApi.executionContext

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
    * Information about the current topology we have in this jvm. Set up by the ClusterLeaderActor
    */
  private[manager] var jvmTopology: JVMTopology = _


  protected val cache: Future[CaffeineCache[Any]] = {
    for {
      clusterInfoCacheSize <- clusterConfigurationApi.clusterInfoCacheSize
      clusterInfoCacheTimeout <- clusterConfigurationApi.clusterInfoCacheTimeout
    } yield {
      val underlying = Caffeine.newBuilder()
        .maximumSize(clusterInfoCacheSize)
        .expireAfterWrite(clusterInfoCacheTimeout, TimeUnit.SECONDS)
        .build[String, Entry[Any]]
      CaffeineCache(underlying)
    }
  }

  /**
    * Ask some information to the Manager. To avoid overloading the remote system, we introduce a cache
    */
  def askClusterInfo(clusterInfo: ClusterInfo)(implicit sender: ActorRef): Future[Option[Any]] = {
    val key = clusterInfo.hashCode().toString
    cache.map(c => c.get(key).transform({
      case Success(r) => Success(r)
      case Failure(e) =>
        logger.error("Impossible to fetch ClusterInfo data from the cache", e)
        Success(None)
    }).flatMap {
      case Some(r) => Future{Option(r)}
      case None =>
        // Info not available in the cache, fetch it from Remote and automatically adds it to cache
        fetchFromManager(clusterInfo)
    }).flatten
  }

  /**
    * Send arbitrary message to Manager and return the result, while putting it in the cache
    * @param clusterInfo
    * @return
    */
  private def fetchFromManager(clusterInfo: ClusterInfo)(implicit sender: ActorRef): Future[Option[Any]] = {
    val key = clusterInfo.hashCode().toString
    manager match {
      case Some(r) =>
        implicit val timeout: Timeout = 5 seconds // Keep an empty line after this one, intellij idea does not parse it correctly otherwise

        r.ask(clusterInfo)(timeout).transformWith{
          case Success(result) =>
            cache.map(c => {
              c.put(key)(result)  // Update the cache
              Option(result)
            })
          case Failure(error) =>
            logger.error(s"Failure while fetching $clusterInfo from Manager.", error)
            Future.successful{
              None
            }
        }
      case None =>
        logger.error(s"Manager not available to fetch $clusterInfo from it.")
        Future{None}
    }
  }

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
        WorkerOrder(fullName, balancer, workerInfo.metadata)
      })
  }

  /**
    * Send all WorkerInfo to the Manager
    */
  private[manager] def notifyWorkerTypeInfo(componentLeader: ComponentLeader)(implicit context: ActorContext): Future[Seq[Any]] = {
    manager match {
      case Some(currentManager) =>
        logger.debug(s"Send all workerTypeInfo to the manager $currentManager")
        val packageAndWorkers = componentLeader.packageLeaders.flatMap(packageLeader => packageLeader.workerInfos.map(workerInfo => (packageLeader, workerInfo)))

        val allMessages = ClusterTools.foldFutures(packageAndWorkers.toIterator, (rawInfo: (PackageLeaderApi, WorkerInfo)) => {
          val packageLeader = rawInfo._1
          val workerInfo = rawInfo._2

          // First send the workerTypeInfo
          val workerTypeInfo: WorkerTypeInfo = convertWorkerInfo(componentLeader.componentName, packageLeader.packageName, workerInfo)
          workerTypeInfo.nodeInfo = nodeInfo.get
          implicit val timeout: Timeout = 10 seconds

          val sendInfo = communication.sendInfoToManager(workerTypeInfo)
              .flatMap(result => {
                // If there is balancer, we can directly send the WorkerOrder to the manager
                loadWorkerOrderFromInfo(componentLeader.componentName, packageLeader.packageName, workerInfo) match {
                  case Some(order) =>
                    communication.sendWorkerOrder(order)
                  case None =>
                    logger.debug(s"No WorkerOrder given for $workerTypeInfo, it must be sent manually afterwards in that case!")
                    Future{Unit}
                }
              })

          sendInfo.andThen {
            case Failure(e) =>
              logger.error(s"Failure while sending the $workerTypeInfo to the manager", e)
          }

          sendInfo
        }, stopOnFailure = true)

        // Make the future fails if there was one failure
        allMessages.map(_.map(_.get))
      case None => // Nothing to do
        logger.error("Not possible to notify the Manager about our WorkerInfo as none is found...")
        Future{throw new MissingManager("Manager not yet available")}
    }
  }

  /**
    * Handle some cluster messages, so nothing related to the election system in itself;
    */
  private[manager] def handleClusterRemoteMessage(componentLeader: ComponentLeader, clusterRemoteMessage: ClusterRemoteMessage)(implicit context: ActorContext): Future[Unit.type] = {
    Future{
      // The entire method is surrounded by a Future to be sure we don't crash for any reason. But we should handle every
      // message correctly by default
      clusterRemoteMessage match {
        case message: StartWorkerActor =>
          handleStartWorker(componentLeader, message)
        case x =>
          logger.error(s"Unknown ClusterRemoteMessage received $x, this should never happen!")
          Future{Unit}
      }
    }.flatten.andThen{
      case Failure(e) =>
        logger.error(s"Impossible to handle the message $clusterRemoteMessage received from the manager", e)
    }
  }

  /**
    * In charge of handling a StartWorkerActor message received from the manager, and try to start it
    * @param message
    */
  private def handleStartWorker(componentLeader: ComponentLeader, message: StartWorkerActor)(implicit context: ActorContext): Future[Unit.type] = {
    // The worker name is not mandatory, it's just to avoid having the developer deals with it if it does not need to
    logger.info(s"Starting worker type id: ${message.workerTypeInfo.workerTypeId}")

    // Find the related packageLeader who can start the worker
    val packageInCharge: Option[PackageLeaderApi] = componentLeader.packageLeaders.find(packageLeader => {
      packageLeader.workerInfos.exists(workerInfo => {
        val fullName = Communication.fullActorName(componentLeader.componentName, packageLeader.packageName, workerInfo.shortName)
        fullName == message.workerTypeInfo.workerTypeId
      })
    })

    packageInCharge match {
      case Some(p) =>
        startWorker(p, message)
      case None =>
        logger.error(s"No PackageLeader available to start the workerTypeId ${message.workerTypeInfo.workerTypeId}")
        Future{throw new MissingPackageLeader(s"No packageLeader available to start ${message.workerTypeInfo.workerTypeId}, this should never happen. Probably an internal error to the library.")}
    }
  }


  private def startWorker(packageLeaderApi: PackageLeaderApi, message: StartWorkerActor)(implicit context: ActorContext): Future[Unit.type] = {
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

            for {
              _ <- communication.sendInfoToManager(m, Option(message.actorRef))
              _ <- communication.sendInfoToManager(workerActorHealth, Option(message.actorRef))
            } yield {
              Unit
            }

          case Failure(e) =>
            logger.error(s"Impossible to set a watcher, the actor probably died mid-way: $e")
            val m = FailedWorkerActor(context.self, message, new Exception("Failing actor after starting it"))
            m.nodeInfo = nodeInfo.get

            communication.sendInfoToManager(m, Option(message.actorRef))
        }
      case Failure(err) =>
        val excep = err match {
          case exception: Exception => exception
          case _ => new Exception(s"Unknown error while starting a workerActor: $err")
        }
        logger.error(s"Got an exception while trying to start a worker $initialName: ${ClusterTools.formatStacktrace(excep)}")
        val m = FailedWorkerActor(context.self, message, excep)
        m.nodeInfo = nodeInfo.get

        communication.sendInfoToManager(m, Option(message.actorRef))
    }
  }
}
