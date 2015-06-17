package mesosphere.marathon.core.launchqueue.impl

import akka.actor.{ Props, Actor, ActorLogging, ActorRef }
import akka.event.LoggingReceive
import mesosphere.marathon.Protos.MarathonTask
import mesosphere.marathon.core.launchqueue.impl.RateLimiterActor.{ AddDelay, DecreaseDelay, DelayUpdate, GetDelay, ResetDelay, ResetDelayResponse }
import mesosphere.marathon.core.task.bus.TaskStatusObservables.TaskStatusUpdate
import mesosphere.marathon.core.task.bus.{ MarathonTaskStatus, TaskStatusObservables }
import mesosphere.marathon.state.{ Timestamp, AppDefinition, AppRepository }
import mesosphere.marathon.tasks.{ TaskIdUtil, TaskTracker }
import org.apache.mesos.Protos.TaskID
import rx.lang.scala.Subscription
import akka.pattern.pipe

import scala.concurrent.Future
import scala.util.control.NonFatal

private[impl] object RateLimiterActor {
  def props(
    rateLimiter: RateLimiter,
    taskTracker: TaskTracker,
    appRepository: AppRepository,
    updateReceiver: ActorRef,
    taskStatusObservables: TaskStatusObservables) = Props(new RateLimiterActor(rateLimiter, taskTracker, appRepository, updateReceiver, taskStatusObservables))

  case class DelayUpdate(app: AppDefinition, delayUntil: Timestamp)

  case class ResetDelay(app: AppDefinition)
  case object ResetDelayResponse

  case class GetDelay(appDefinition: AppDefinition)
  private case class AddDelay(app: AppDefinition)
  private case class DecreaseDelay(app: AppDefinition)
}

private[impl] class RateLimiterActor private (
    rateLimiter: RateLimiter,
    taskTracker: TaskTracker,
    appRepository: AppRepository,
    updateReceiver: ActorRef,
    taskStatusObservables: TaskStatusObservables) extends Actor with ActorLogging {
  var taskStatusSubscription: Subscription = _

  override def preStart(): Unit = {
    taskStatusSubscription = taskStatusObservables.forAll.subscribe(self ! _)
    log.info("started RateLimiterActor")
  }

  override def postStop(): Unit = {
    taskStatusSubscription.unsubscribe()
  }

  override def receive: Receive = LoggingReceive {
    case GetDelay(app) =>
      sender() ! DelayUpdate(app, rateLimiter.getDelay(app))

    case AddDelay(app) =>
      rateLimiter.addDelay(app)
      updateReceiver ! DelayUpdate(app, rateLimiter.getDelay(app))

    case ResetDelay(app) =>
      rateLimiter.resetDelay(app)
      updateReceiver ! DelayUpdate(app, rateLimiter.getDelay(app))
      sender() ! ResetDelayResponse

    case TaskStatusUpdate(_, taskId, status) =>
      status match {
        case MarathonTaskStatus.Error(_) | MarathonTaskStatus.Lost(_) |
          MarathonTaskStatus.Failed(_) | MarathonTaskStatus.Finished(_) =>

          sendToSelfForApp(taskId, AddDelay(_))

        case MarathonTaskStatus.Running(mesosStatus) if mesosStatus.forall(s => !s.hasHealthy || s.getHealthy) =>
          sendToSelfForApp(taskId, DecreaseDelay(_))

        case _ => // Ignore
      }

    case unknown: Any => log.warning("Unhandled message: {}", unknown)
  }

  private[this] def sendToSelfForApp(taskId: TaskID, toMessage: AppDefinition => Any): Unit = {
    val appId = TaskIdUtil.appId(taskId)
    val maybeTask: Option[MarathonTask] = taskTracker.fetchTask(appId, taskId.getValue)
    val maybeAppFuture: Future[Option[AppDefinition]] = maybeTask match {
      case Some(task) =>
        val version: Timestamp = Timestamp(task.getVersion)
        val appFuture = appRepository.app(appId, version)
        import context.dispatcher
        appFuture.foreach {
          case None => log.info("App '{}' with version '{}' not found. Outdated?", appId, version)
          case _    =>
        }
        appFuture.recover {
          case NonFatal(e) =>
            log.error("error while retrieving app '{}', version '{}'", appId, version, e)
            None
        }
      case None =>
        log.warning("Received update for unknown task '{}'", taskId.getValue)
        Future.successful(None)
    }

    import context.dispatcher
    maybeAppFuture.foreach {
      case Some(app) => self ! toMessage(app)
      case None      => // nothing
    }
  }
}
