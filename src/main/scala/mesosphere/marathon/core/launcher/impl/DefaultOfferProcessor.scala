package mesosphere.marathon.core.launcher.impl

import mesosphere.marathon.core.base.Clock
import mesosphere.marathon.core.launcher.{ OfferProcessor, OfferProcessorConfig, TaskLauncher }
import mesosphere.marathon.core.matcher.OfferMatcher
import mesosphere.marathon.core.matcher.OfferMatcher.MatchedTasks
import mesosphere.marathon.metrics.{ MetricPrefixes, Metrics }
import org.apache.mesos.Protos.Offer
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.concurrent.duration._

private[impl] class DefaultOfferProcessor(
    conf: OfferProcessorConfig, clock: Clock,
    metrics: Metrics,
    offerMatcher: OfferMatcher, taskLauncher: TaskLauncher) extends OfferProcessor {
  import scala.concurrent.ExecutionContext.Implicits.global

  private[this] val log = LoggerFactory.getLogger(getClass)
  private[this] val offerMatchingTimeout = conf.offerMatchingTimeout().millis

  private[this] val incomingOffers = metrics.meter(metrics.name(MetricPrefixes.SERVICE, getClass, "incomingOffers"))
  private[this] val matchTime = metrics.timer(metrics.name(MetricPrefixes.SERVICE, getClass, "matchTime"))

  override def processOffer(offer: Offer): Unit = {
    incomingOffers.mark()

    val deadline = clock.now() + offerMatchingTimeout

    val matchFuture: Future[MatchedTasks] = matchTime.futureSuccess {
      offerMatcher.processOffer(deadline, offer)
    }

    matchFuture.map {
      case MatchedTasks(offerId, tasks) =>
        if (tasks.nonEmpty) {
          if (taskLauncher.launchTasks(offerId, tasks.map(_.taskInfo))) {
            log.info("task launch successful for {}", offerId.getValue)
            tasks.foreach(_.accept())
          }
          else {
            log.info("task launch rejected for {}", offerId.getValue)
            tasks.foreach(_.reject())
          }
        }
        else {
          taskLauncher.declineOffer(offerId)
        }
    }
  }
}
