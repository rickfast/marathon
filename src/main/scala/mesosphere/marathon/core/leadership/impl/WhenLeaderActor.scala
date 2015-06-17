package mesosphere.marathon.core.leadership.impl

import akka.actor.{ Actor, ActorLogging, ActorRef, PoisonPill, Props, Stash, Status, Terminated }
import akka.event.LoggingReceive
import mesosphere.marathon.core.leadership.PreparationMessages.{ PrepareForStart, Prepared }
import mesosphere.marathon.core.leadership.impl.WhenLeaderActor.{ Stop, Stopped }

private[impl] object WhenLeaderActor {
  def props(childProps: Props, preparedOnStart: Boolean = true): Props = {
    Props(
      new WhenLeaderActor(childProps, preparedOnStart)
    )
  }

  case object Stop
  case object Stopped
}

/**
  * Wraps an actor which is only started when we are currently the leader.
  */
private class WhenLeaderActor(childProps: => Props, preparedOnStart: Boolean)
    extends Actor with ActorLogging with Stash {

  private[this] var leadershipCycle = 1

  override def receive: Receive = suspended

  private[this] def suspended: Receive = LoggingReceive.withLabel("suspended") {
    case PrepareForStart(coordinatorRef) =>
      val childRef = context.actorOf(childProps, leadershipCycle.toString)
      leadershipCycle += 1
      if (preparedOnStart) {
        coordinatorRef ! Prepared(self)
        context.become(active(childRef))
      }
      else {
        context.become(starting(coordinatorRef, childRef))
      }

    case Stop           => // nothing changes

    case unhandled: Any => sender() ! Status.Failure(new IllegalStateException("not currently leader"))
  }

  private[this] def starting(coordinatorRef: ActorRef, childRef: ActorRef): Receive = LoggingReceive.withLabel("starting") {
    case Prepared(`childRef`) =>
      coordinatorRef ! Prepared(self)
      unstashAll()
      context.become(active(childRef))

    case Stop           => stop(childRef)

    case unhandled: Any => stash()
  }

  private[this] def active(childRef: ActorRef): Receive = LoggingReceive.withLabel("active") {
    case Stop           => stop(childRef)
    case unhandled: Any => childRef.forward(unhandled)
  }

  private[this] def stop(childRef: ActorRef): Unit = {
    context.watch(childRef)
    childRef ! PoisonPill
    unstashAll()
    context.become(dying(sender(), childRef))
  }

  private[this] def dying(stopAckRef: ActorRef, childRef: ActorRef): Receive = LoggingReceive.withLabel("dying") {
    case Terminated(`childRef`) =>
      unstashAll()
      stopAckRef ! Stopped
      context.become(suspended)

    case unhandled: Any => stash()
  }
}
