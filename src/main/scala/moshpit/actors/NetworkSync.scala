package moshpit.actors

import com.roundeights.hasher.Implicits._
import akka.actor.{Actor, ActorRef, Props}
import akka.event.Logging
import com.roundeights.hasher.Digest
import moshpit.VClock
import moshpit.actors.NetworkSync.Tasks.AdvertiseRootTask
import cats._
import cats.data._
import cats.implicits._
import scala.util.{Success, Failure}


object NetworkSync {
  def props(ourGuid:String, seeds:Seq[String], appDbRef:ActorRef) = Props(new NetworkSync(ourGuid, seeds, appDbRef))

  object Messages {
    case class AdvertiseRootHash(hash:Digest) extends P2p.P2pMessagePayload
    case class RequestApps(hash:Digest) extends P2p.P2pMessagePayload
    case class PushApps(apps:Map[String, Digest]) extends P2p.P2pMessagePayload
    case class RequestInstancesMeta(appIds:Set[String]) extends P2p.P2pMessagePayload
    case class PushInstancesMeta(apps:Map[String, Map[String, VClock]]) extends P2p.P2pMessagePayload
    case class RequestFullInstance(appId:String, instanceGuid:String) extends P2p.P2pMessagePayload
    case class PushFullInstance(appId:String, instanceGuid:String,
                                instanceMetaInfo: InstanceMetaInfo, userData:String) extends P2p.P2pMessagePayload
  }

  object Tasks {
    case class AdvertiseRootTask()
  }
}

// n1                                  n2
// ----- advertise root hash ----------->  // hash of all vclocks in the system and metainfo
// <---- request apps -------------------
// ----- apps [appId, hash] ------------>  // appid, hash of instances vclocks and metainfos
// <---- request app (appId) ------------
// -- app instances [metainfo(vclock)] ->  // app instance vclock
// <-- request instance -----------------
// --- instance (full) ----------------->  // full instance with metainfo and userdata


class NetworkSync(ourGuid:String, seeds:Seq[String], appDbRef:ActorRef) extends Actor {
  import NetworkSync._
  private val log =  Logging(context.system, this)
  private val p2p = context.actorOf(P2p.props(ourGuid, seeds), "p2p")
  val appDbProxy = new AppDbProxy(appDbRef)
  override def preStart(): Unit = p2p ! P2p.Messages.Subscribe(context.self)
  import context.dispatcher

  override def receive: Receive = {
    case Tasks.AdvertiseRootTask() =>
      appDbProxy.queryRootHash().map(ourHash =>
        p2p ! P2p.Messages.Broadcast(Messages.AdvertiseRootHash(ourHash))
      )

    case P2p.NetMessages.Message(sender, payload) => payload match {
      case Messages.AdvertiseRootHash(theirHash) =>
        appDbProxy.queryRootHash().map(ourHash => {
          if (ourHash != theirHash) {
            p2p ! P2p.Messages.Send(sender, Messages.RequestApps(theirHash))
          }
        })

      case Messages.RequestApps(prevHash) =>
        appDbProxy.queryApps().map(apps => {
          p2p ! P2p.Messages.Send(sender, Messages.PushApps(apps))
        })

      case Messages.PushApps(theirApps) =>
        appDbProxy.queryApps().map(ourApps => {
          val our = ourApps.toSet
          val their = theirApps.toSet
          val toSync = their.diff(our) // our stuff that is different will be synced there by that node ///our.union(their).diff(our.intersect(their))
          p2p ! P2p.Messages.Send(sender, Messages.RequestInstancesMeta(toSync.map(_._1)))
        })

      case Messages.RequestInstancesMeta(appIds) =>
        appIds.map(appId => appDbProxy.queryApp(appId).map(r => (appId, r))).toList.sequenceU.map(res =>
          p2p ! P2p.Messages.Send(sender, Messages.PushInstancesMeta(res.toMap))
        )

      case Messages.PushInstancesMeta(theirApps) =>
        theirApps.keys.foreach(appId => appDbProxy.queryApp(appId).map(ourInstances => {
          val theirInstances = theirApps(appId).toSet
          val toSync = theirInstances.diff(ourInstances.toSet)
          toSync//.filterNot(i => ourInstances.get(i._1).exists(v => v.isSubclockOf(i._2))) // if theirs is future of time of ours
            .foreach(s => {
            log.info(s"requesting full instance $appId::${s._1}")
            p2p ! P2p.Messages.Send(sender, Messages.RequestFullInstance(appId, s._1))
          })
        }))

      case Messages.RequestFullInstance(appId, instanceGuid) =>
        log.info(s"got request for full instance $appId::$instanceGuid")
        appDbProxy.queryInstance(appId, instanceGuid).andThen({
          case Success(AppDb.Messages.QueryInstance.Success(meta, data)) =>
            p2p ! P2p.Messages.Send(sender, Messages.PushFullInstance(appId, instanceGuid, meta, data))
          case Success(AppDb.Messages.QueryInstance.NotExists()) =>
            log.warning("requested non existing instance, strange")
        })

      case Messages.PushFullInstance(appId, instanceGuid, meta, data) =>
        log.info(s"got full instance for $appId and $instanceGuid with meta $meta")
        appDbProxy.syncInstance(appId, instanceGuid, meta, data)
    }
  }
}
