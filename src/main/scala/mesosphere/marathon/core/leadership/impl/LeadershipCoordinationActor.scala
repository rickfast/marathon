package mesosphere.marathon.core.leadership.impl

import akka.actor.{ Actor, ActorLogging, ActorRef, Props, Stash, Status, Terminated }
import akka.event.LoggingReceive
import mesosphere.marathon.core.leadership.PreparationMessages
import mesosphere.marathon.core.leadership.impl.WhenLeaderActor.{ Stopped, Stop }

private[impl] object LeadershipCoordinationActor {
  def props(whenLeaderActors: Set[ActorRef]): Props = {
    Props(new LeadershipCoordinationActor(whenLeaderActors))
  }
}

private class LeadershipCoordinationActor(var whenLeaderActors: Set[ActorRef])
    extends Actor with ActorLogging with Stash {

  override def receive: Receive = suspended

  private[this] def suspended: Receive = {
    log.info("All actors suspended:\n{}", whenLeaderActors.map(actorRef => s"* $actorRef").mkString("\n"))

    LoggingReceive.withLabel("suspended") {
      case Terminated(actorRef) =>
        log.error("unexpected death of {}", actorRef)
        whenLeaderActors -= actorRef

      case PreparationMessages.PrepareForStart =>
        whenLeaderActors.foreach(_ ! PreparationMessages.PrepareForStart(self))
        context.become(preparingForStart(sender(), whenLeaderActors))

      case WhenLeaderActor.Stop => // nothing changes

      case unhandled: Any       => sender() ! Status.Failure(new IllegalStateException("not currently leader"))
    }
  }

  private[this] def preparingForStart(ackStartRef: ActorRef, whenLeaderActorsWithoutAck: Set[ActorRef]): Receive = {
    if (whenLeaderActorsWithoutAck.isEmpty) {
      ackStartRef ! PreparationMessages.Prepared(self)
      active
    }
    else {
      LoggingReceive.withLabel("preparingForStart") {
        case PreparationMessages.PrepareForStart => // nothing changes
        case PreparationMessages.Prepared(whenLeaderRef) =>
          context.become(preparingForStart(ackStartRef, whenLeaderActorsWithoutAck - whenLeaderRef))

        case Terminated(actorRef) =>
          log.error("unexpected death of {}", actorRef)
          whenLeaderActors -= actorRef
          context.become(preparingForStart(ackStartRef, whenLeaderActorsWithoutAck - actorRef))

        case WhenLeaderActor.Stop =>
          whenLeaderActors.foreach(_ ! Stop)
          context.become(suspended)
      }
    }
  }

  private[this] def active: Receive = {
    log.info("All actors active:\n{}", whenLeaderActors.map(actorRef => s"* $actorRef").mkString("\n"))

    LoggingReceive.withLabel("active") {
      case Terminated(actorRef) =>
        log.error("unexpected death of {}", actorRef)
        whenLeaderActors -= actorRef

      case PreparationMessages.PrepareForStart => // nothing changes

      case WhenLeaderActor.Stop =>
        whenLeaderActors.foreach(_ ! Stop)
        context.become(suspended)
    }
  }
}
