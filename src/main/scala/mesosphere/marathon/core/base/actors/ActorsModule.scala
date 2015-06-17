package mesosphere.marathon.core.base.actors

import akka.actor.{ Scheduler, ActorRefFactory, ActorSystem }
import mesosphere.marathon.core.base.ShutdownHooks
import org.slf4j.LoggerFactory
import scala.concurrent.duration._

/**
  * Contains basic dependencies used throughout the application disregarding the concrete function.
  */
trait ActorsModule {
  def actorRefFactory: ActorRefFactory
}

object ActorsModule {
  def apply(shutdownHooks: ShutdownHooks, actorSystem: ActorSystem = ActorSystem()): ActorsModule =
    new DefaultActorsModule(shutdownHooks, actorSystem)
}

private class DefaultActorsModule(shutdownHooks: ShutdownHooks, actorSystem: ActorSystem) extends ActorsModule {
  private[this] val log = LoggerFactory.getLogger(getClass)

  override def actorRefFactory: ActorRefFactory = actorSystem

  shutdownHooks.onShutdown {
    log.info("Shutting down actor system")
    actorSystem.shutdown()
    actorSystem.awaitTermination(10.seconds)
  }
}
