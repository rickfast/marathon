package mesosphere.marathon.core.launcher.impl

import mesosphere.marathon.MarathonSchedulerDriverHolder
import mesosphere.marathon.core.base.Clock
import mesosphere.marathon.core.launcher.{ OfferProcessorConfig, LauncherModule, OfferProcessor, TaskLauncher }
import mesosphere.marathon.core.matcher.OfferMatcher
import mesosphere.marathon.metrics.Metrics

private[launcher] class DefaultLauncherModule(
  clock: Clock,
  metrics: Metrics,
  offerProcessorConfig: OfferProcessorConfig,
  marathonSchedulerDriverHolder: MarathonSchedulerDriverHolder,
  offerMatcher: OfferMatcher)
    extends LauncherModule {

  override lazy val offerProcessor: OfferProcessor =
    new DefaultOfferProcessor(
      offerProcessorConfig, clock,
      metrics,
      offerMatcher, taskLauncher)

  override lazy val taskLauncher: TaskLauncher = new DefaultTaskLauncher(
    marathonSchedulerDriverHolder,
    clock)
}
