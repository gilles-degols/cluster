package net.degols.libs.cluster.manager

import akka.actor.{Actor, ActorRef, Cancellable, Kill, Props, Terminated}
import com.google.inject.{Inject, Singleton}
import net.degols.libs.cluster.balancing.LoadBalancer
import net.degols.libs.cluster.core.Cluster
import net.degols.libs.cluster.ClusterTools
import net.degols.libs.cluster.configuration.{ClusterConfiguration, ClusterConfigurationApi, DefaultClusterConfiguration}
import net.degols.libs.cluster.messages._
import net.degols.libs.cluster.utils.PriorityStashedActor
import net.degols.libs.election._
import org.slf4j.LoggerFactory
import play.api.libs.json.{JsObject, Json}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

/**
  * Internal message to verify if we might be re-connected to a manager after a short network problem
  */
case object IsStillDisconnectedFromManager

/**
  * Local class instantiated by the developer to give information about each Worker to start.
  * @param shortName short version of the full WorkerTypeId. Automatically the full WorkerTypeId will be computed the
  *                  following way s"$COMPONENT:$PACKAGE:$shortName".
  * @param balancerType indicates how to balance the actors of the given type. If present, the related order to start
  *                 the Workers for the WorkerInfo will be automatically sent.
  *                 If not provided, the developer needs to manually send WorkerOrder to the manager to start them
  * @param metadata Additional metadata that can sent to the Manager. If present, it is most likely used to for a specific
  *                 loadBalancer (some parameters to pass along)
  */
case class WorkerInfo(shortName: String, balancerType: Option[LoadBalancerType] = None, metadata: JsObject = Json.obj())

/**
  * Local class instantiated by the developer to ask for some Workers of a specific WorkerTypeId to be started.
  * If the related WorkerInfo is not yet present, it will wait for them to appear to have the WorkerOrder automatically
  * executed by the manager, no need to handle that case directly.
  * @param fullName Complete name of the worker, in the format s"$COMPONENT:$PACKAGE:$shortName"
  * @param balancerType Balancer to indicate how many workers we want to be started
  * @param metadata Additional metadata (different than the one from the WorkerInfo) to allow the load balancer to be
  *                 customized
  * @param id most of the time it is not needed to specify it, as it allows to have different sub-groups of the same
  *           type of WorkerType (maybe with different load balancer). This id MUST be completely unique, so it should
  *           at least contain parts of the "fullName" variable for example.
  */
case class WorkerOrder(fullName: String, balancerType: LoadBalancerType, metadata: JsObject = Json.obj(), id: Option[String] = None)


/**
  * Manage the various Workers in the current JVM, only one instance is allowed, hence the Singleton. This is the actor
  * knowing which WorkerActors are available, and how to start them.
  */
@Singleton
class ClusterLeaderActor @Inject()(
                                    componentLeaderApi: ComponentLeaderApi, // Implemented by the developer
                                    clusterConfigurationApi: ClusterConfigurationApi, // The developer can override it
                                    service: ClusterServiceLeader,
                                    electionService: ElectionService,
                                    configurationService: ConfigurationService) extends PriorityStashedActor{
  private val logger = LoggerFactory.getLogger(getClass)


  /**
    * Local Manager in charge of the election. It's not necessarily the manager in charge.
    * It is created after we initialze the ClusterConfiguration, PackageLeaders & LoadBalancers
    */
  var localManager: Option[ActorRef] = None

  /**
    * Scheduled message to verify if we are still disconnected from the manager in charge after some time. If yes, we
    * need to kill every actor of our current jvm
    */
  private var checkManagerDisconnection: Option[Cancellable] = None

  /**
    * Easier-to-use configuration
    */
  private var _clusterConfiguration: Option[ClusterConfiguration] = None

  /**
    * Easier-to-use ComponentLeader
    */
  private var _componentLeader: Option[ComponentLeader] = None

  private var _cluster: Option[Cluster] = None

  /**
    * Load configuration and setup the different packages & their workers
    */
  def load(): Future[(ClusterConfiguration, Seq[PackageLeaderApi], Seq[LoadBalancer])] = {
    for {
      // Loading the configuration
      clusterConfiguration <- ClusterConfiguration.load(clusterConfigurationApi)

      // Put the reference to ClusterServiceLeader for each PackageLeader
      packageLeaders <- componentLeaderApi.packageLeaders.map(packageLeaders => {
        packageLeaders.foreach(pck => {
          pck.setContext(context)
          pck.setClusterServiceLeader(service)
        })
        packageLeaders
      })

      // Setting up the workers
      setupWorkers <- ClusterTools.foldFutures(packageLeaders.toIterator, (pck: PackageLeaderApi) => {
        pck.setupWorkers().andThen{
          case Success(r) => logger.debug(s"Workers setup done for package $pck")
          case Failure(e) => logger.error(s"Impossible to setup the worker for package $pck", e)
        }
      })

      componentLeader <- ComponentLeader.from(componentLeaderApi)

      // Get the different load balancers
      loadBalancers <- componentLeaderApi.loadBalancers
    } yield {
      service.nodeInfo = Option {
        val networkHostname = ClusterTools.remoteActorPath(self).split("@")(1).split(":").head
        val localHostname = clusterConfiguration.localHostname
        NodeInfo(networkHostname, localHostname)
      }

      service.jvmTopology = JVMTopology(self)
      _clusterConfiguration = Option(clusterConfiguration)
      _componentLeader = Option(componentLeader)
      _cluster = Option(new Cluster(clusterConfiguration))

      (clusterConfiguration, packageLeaders, loadBalancers)
    }
  }

  override def preStart(): Unit = {
    super.preStart()

    // As the local manager is only started after the initialization is successfully done, there is no need to have like
    // a "StartActor" message with different states.
    load().transform{
      case Success(r) =>
        logger.debug("Loaded the default cluster configuration, packageLeaders and their workers. We try to start the local manager")

        localManager = Option(context.actorOf(Props.create(classOf[Manager], electionService, configurationService, r._1, _cluster.get), name = "LocalManager"))
        localManager.get ! IAmTheWorkerLeader(r._3)

        Success(r)
      case Failure(e) =>
        logger.error("Impossible to initialize the default configuration, different packageLeaders & their workers", e)
        self ! Kill
        Failure(e)
    }
  }

  /**
    *
    * @return
    */
  def starting: Receive = {
    case StartActor =>
      logger.debug("Received 'StartActor' message for the ClusterLeaderActor, initializing...")
    case other =>
      logger.error(s"Received unknown message while being in the state 'starting': $other")
  }


  // TODO: Add suicide when we didn't get a new Manager in a short amount of time. Or better: send a message to all
  // actors to stop their work, and it will be resumed by the manager
  override def receive: Receive = {
    case message: TheLeaderIs => // We only receive a "TheLeaderIs" if the state changed
      logger.warn(s"Got a TheLeaderIs message from the manager: $message")
      val f = Future {
        service.manager = message.leaderWrapper // message.leader is only used for the election != cluster management as the cluster management owns the election actor.
        // We need to send all worker type info (even if the manager has just switched, we don't care)
        service.notifyWorkerTypeInfo(_componentLeader.get)

        // Set up of the packageLeaders is done, we can execute the post-start of the packageLeaders (most of the time
        // they contain nothing)
        message.leader.map(x => {
          ClusterTools.foldFutures(_componentLeader.get.packageLeaders.toIterator, (pck: PackageLeaderApi) => {
            pck.postManagerConnection()
          })
        }).getOrElse(Future.successful{})
      }.flatten.andThen{
        case Success(r) => // Nothing to do
        case Failure(e) =>
          logger.error("Impossible to send all the WorkerTypeInfo and/or executing the postManagerConnection for every packageLeader", e)
      }

      endProcessing(message, f)

    case clusterMessage: ClusterRemoteMessage =>
      // Message used for the administration, we execute it
      service.handleClusterRemoteMessage(_componentLeader.get, clusterMessage)

      endProcessing(clusterMessage)

    case IsStillDisconnectedFromManager =>
      service.manager match {
        case Some(m) =>
          logger.warn("We just re-check if we are still disconnected from the Manager and it seems we got it back. We let workers continue their job. Be sure to have implemented a hard-load-balancing in charge of killing workers in excess.")
        case None =>
          logger.warn("We just re-check if we are still disconnected from the Manager and it seems we still don't have it, so we kill all our workers.")
          service.jvmTopology.workerActors.values.flatMap(_.map(_.actorRef)).foreach(_ ! Kill)
      }

      endProcessing(IsStillDisconnectedFromManager)

    case terminatingActor: Terminated =>
      Try {
        // We watch our own actors, but also the Manager
        if(service.jvmTopology.removeWorkerActor(terminatingActor.actor)) {
          logger.warn(s"Got a Terminated message from a WorkerActor (${terminatingActor.actor}), it has been removed from our jvm topology.")
        } else if(service.manager.isDefined && service.manager.get == terminatingActor.actor) {
          checkManagerDisconnection.map(c => c.cancel())
          logger.warn(s"Got a Terminated message from the Manager, we let the actors continue their job during ${_clusterConfiguration.get.watcherTimeoutBeforeSuicide.toSeconds} seconds and wait for a new Manager to come in.")
          checkManagerDisconnection = Option(context.system.scheduler.scheduleOnce(_clusterConfiguration.get.watcherTimeoutBeforeSuicide, self, IsStillDisconnectedFromManager))
        } else {
          logger.error(s"Got a Terminated message from a unknown actor: ${terminatingActor.actor}")
        }
      } match {
        case Success(r) =>
        case Failure(e) =>
          logger.error(s"Problem while logging a Terminated Actor: $terminatingActor", e)
      }

      endProcessing(terminatingActor)

    case x =>
      logger.error(s"Unknown message: $x . Note that you should not use the WorkerLeader for your own messages as they are not forwarded.")
      endProcessing(x)
  }

}
