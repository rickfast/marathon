package mesosphere.marathon.core.launcher

import mesosphere.marathon.MarathonSchedulerDriverHolder
import mesosphere.marathon.core.base.Clock
import mesosphere.marathon.core.launcher.impl.DefaultLauncherModule
import mesosphere.marathon.core.matcher.OfferMatcher
import mesosphere.marathon.metrics.Metrics

trait LauncherModule {
  def offerProcessor: OfferProcessor
  def taskLauncher: TaskLauncher
}

object LauncherModule {
  def apply(
    clock: Clock, metrics: Metrics,
    offerProcessorConfig: OfferProcessorConfig,
    marathonSchedulerDriverHolder: MarathonSchedulerDriverHolder,
    offerMatcher: OfferMatcher): LauncherModule =
    new DefaultLauncherModule(clock, metrics, offerProcessorConfig, marathonSchedulerDriverHolder, offerMatcher)
}
