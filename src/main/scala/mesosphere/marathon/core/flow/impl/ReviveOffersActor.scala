package mesosphere.marathon.core.flow.impl

import akka.actor.{ Actor, ActorLogging, Props }
import mesosphere.marathon.MarathonSchedulerDriverHolder
import rx.lang.scala.{ Observable, Subscription }

object ReviveOffersActor {
  def props(offersWanted: Observable[Boolean], driverHolder: MarathonSchedulerDriverHolder): Props = {
    Props(new ReviveOffersActor(offersWanted, driverHolder))
  }
}

private class ReviveOffersActor(offersWanted: Observable[Boolean], driverHolder: MarathonSchedulerDriverHolder)
    extends Actor with ActorLogging {
  private[this] var subscription: Subscription = _
  private[this] var previouslyWanted: Boolean = false

  override def preStart(): Unit = {
    subscription = offersWanted.subscribe(self ! _)
  }

  override def postStop(): Unit = {
    subscription.unsubscribe()
  }

  override def receive: Receive = {
    case true if !previouslyWanted =>
      log.debug("Revive offers")
      driverHolder.driver.foreach(_.reviveOffers())
      previouslyWanted = true

    case bool: Boolean => previouslyWanted = bool
  }
}
