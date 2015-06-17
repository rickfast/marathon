package mesosphere.marathon.core.matcher.manager.impl

import akka.actor.ActorRef
import mesosphere.marathon.core.base.Clock
import mesosphere.marathon.core.leadership.LeadershipModule
import mesosphere.marathon.core.matcher.OfferMatcher
import mesosphere.marathon.core.matcher.manager.{ OfferMatcherConfig, OfferMatcherManager, OfferMatcherManagerModule }
import mesosphere.marathon.core.matcher.util.ActorOfferMatcher
import mesosphere.marathon.metrics.Metrics
import rx.lang.scala.subjects.AsyncSubject
import rx.lang.scala.{ Observable, Subject }

import scala.util.Random

private[matcher] class DefaultOfferMatcherManagerModule(
    clock: Clock, random: Random, metrics: Metrics,
    offerMatcherConfig: OfferMatcherConfig,
    leadershipModule: LeadershipModule) extends OfferMatcherManagerModule {

  private[this] lazy val offersWanted: Subject[Boolean] = AsyncSubject[Boolean]()

  private[this] val offerMatcherMultiplexer: ActorRef = {
    val props = OfferMatcherManagerActor.props(random, clock, offerMatcherConfig, offersWanted)
    leadershipModule.startWhenLeader(props, "OfferMatcherMultiplexer")
  }

  override val globalOfferMatcherWantsOffers: Observable[Boolean] = offersWanted
  override val globalOfferMatcher: OfferMatcher = new ActorOfferMatcher(clock, offerMatcherMultiplexer)
  override val subOfferMatcherManager: OfferMatcherManager = new ActorOfferMatcherManager(offerMatcherMultiplexer)
}
