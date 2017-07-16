package moshpit.actors

import com.roundeights.hasher.Implicits._
import akka.actor.{Actor, ActorRef, Props}
import akka.event.Logging
import com.roundeights.hasher.Digest
import moshpit.VClock
import moshpit.actors.NetworkSync.Tasks.AdvertiseRootTask


object NetworkSync {
  def props(ourGuid:String, seeds:Seq[String], appDbRef:ActorRef) = Props(new NetworkSync(ourGuid, seeds, appDbRef))

  object Messages {
    case class AdvertiseRootHash(hash:Digest) extends P2p.P2pMessagePayload
    case class AdvertiseAppHash(hash:Digest) extends P2p.P2pMessagePayload
    case class RequestApps(hash:Digest) extends P2p.P2pMessagePayload
    case class SyncApps(appIds:Set[String]) extends P2p.P2pMessagePayload
    case class RequestInstances() extends P2p.P2pMessagePayload
    case class SyncInstances(instances:Map[String, (VClock, String)]) extends P2p.P2pMessagePayload
  }

  object Tasks {
    case class AdvertiseRootTask()
  }
}


class NetworkSync(ourGuid:String, seeds:Seq[String], appDbRef:ActorRef) extends Actor {
  import NetworkSync._
  private val log =  Logging(context.system, this)
  private val p2p = context.actorOf(P2p.props(ourGuid, seeds), "p2p")
  val appDbProxy = new AppDbProxy(appDbRef)
  override def preStart(): Unit = p2p ! P2p.Messages.Subscribe(context.self)

  def hashSetOfStrings(xs:Set[String])= xs.mkString(",").md5
  def getOurHash() = appDbProxy.queryApps().map(appIds => hashSetOfStrings(appIds))

  override def receive: Receive = {
    case Tasks.AdvertiseRootTask() =>
      getOurHash().map(ourHash =>
        p2p ! P2p.Messages.Broadcast(Messages.AdvertiseRootHash(ourHash))
      )

    case P2p.NetMessages.Message(sender, payload) => payload match {
      case Messages.AdvertiseRootHash(theirsHash) =>
        getOurHash().map(oursHash => {
          if (oursHash != theirsHash) {
            p2p ! P2p.Messages.Send(sender, Messages.RequestApps(theirsHash))
          }
        })

      case Messages.RequestApps(prevHash) =>
        appDbProxy.queryApps().map(appIds => {
          val oursHash = hashSetOfStrings(appIds)
          if (oursHash != prevHash) {
            log.info("hash mismatch, waiting till next advert")
          } else {
            log.info("hash match, syncing apps")
            p2p ! P2p.Messages.Send(sender, Messages.SyncApps(appIds))
          }
        })

      case Messages.SyncApps
    }
  }
}
