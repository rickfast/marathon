package mesosphere.marathon.core.matcher.manager

import mesosphere.marathon.core.base.Clock
import mesosphere.marathon.core.leadership.LeadershipModule
import mesosphere.marathon.core.matcher.OfferMatcher
import mesosphere.marathon.core.matcher.manager.impl.DefaultOfferMatcherManagerModule
import mesosphere.marathon.metrics.Metrics
import rx.lang.scala.Observable

import scala.util.Random

trait OfferMatcherManagerModule {
  /** The offer matcher which forwards match requests to all registered sub offer matchers. */
  def globalOfferMatcher: OfferMatcher
  def globalOfferMatcherWantsOffers: Observable[Boolean]
  def subOfferMatcherManager: OfferMatcherManager
}

object OfferMatcherManagerModule {
  def apply(
    clock: Clock,
    random: Random, metrics: Metrics,
    offerMatcherConfig: OfferMatcherConfig,
    leadershipModule: LeadershipModule): OfferMatcherManagerModule =
    new DefaultOfferMatcherManagerModule(clock, random, metrics, offerMatcherConfig, leadershipModule)
}

