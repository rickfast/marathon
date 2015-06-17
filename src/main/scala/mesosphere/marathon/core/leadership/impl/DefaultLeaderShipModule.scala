package mesosphere.marathon.core.leadership.impl

import akka.actor.{ ActorRef, ActorRefFactory, Props }
import mesosphere.marathon.core.leadership.{ LeadershipCoordinator, LeadershipModule }

private[leadership] class DefaultLeaderShipModule(actorRefFactory: ActorRefFactory)
    extends LeadershipModule {

  private[this] var whenLeaderRefs = Set.empty[ActorRef]
  private[this] var started: Boolean = false

  override def startWhenLeader(props: => Props, name: String, preparedOnStart: Boolean = true): ActorRef = {
    require(!started, "already started")
    val proxyProps = WhenLeaderActor.props(props)
    val actorRef = actorRefFactory.actorOf(proxyProps, name)
    whenLeaderRefs += actorRef
    actorRef
  }

  private[this] lazy val coordinator_ = {
    require(!started, "already started")
    started = true

    val props = LeadershipCoordinationActor.props(whenLeaderRefs)
    val actorRef = actorRefFactory.actorOf(props, "leaderShipCoordinator")
    new ActorLeadershipCoordinator(actorRef)
  }

  override def coordinator(): LeadershipCoordinator = coordinator_
}
