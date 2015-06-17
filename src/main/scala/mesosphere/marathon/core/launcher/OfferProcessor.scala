package mesosphere.marathon.core.launcher

import org.apache.mesos.Protos.Offer

trait OfferProcessor {
  def processOffer(offer: Offer)
}
