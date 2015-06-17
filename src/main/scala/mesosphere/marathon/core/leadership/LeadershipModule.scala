package mesosphere.marathon.core.leadership

import akka.actor.{ ActorRef, ActorRefFactory, Props }
import mesosphere.marathon.core.leadership.impl.DefaultLeaderShipModule

trait LeadershipModule {
  /**
    * Starts the given top-level actor when we are leader. Otherwise reject all messages
    * to the actor by replying with Failure messages.
    *
    * The returned ActorRef stays stable across leadership changes.
    *
    * @param considerPreparedOnStart
    *                        if true, treat the actor as started as soon as we have created its actorRef.
    *                        if false, send [[LeadershipModule#PrepareToStart]] and wait for answer
    *                                  before considering the actor as started.
    */
  def startWhenLeader(props: => Props, name: String, considerPreparedOnStart: Boolean = true): ActorRef
  /** Returns the LeadershipCoordinator. Must be called after all startWhenLeader calls. */
  def coordinator(): LeadershipCoordinator
}

object LeadershipModule {
  def apply(actorRefFactory: ActorRefFactory): LeadershipModule = {
    new DefaultLeaderShipModule(actorRefFactory)
  }
}
