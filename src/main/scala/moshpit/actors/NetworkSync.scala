package moshpit.actors

import com.roundeights.hasher.Implicits._
import akka.actor.{Actor, ActorRef, Cancellable, Props}
import akka.event.Logging
import com.roundeights.hasher.Hash
import moshpit.VClock
import moshpit.actors.NetworkSync.Tasks.AdvertiseRootTask
import cats._
import cats.data._
import cats.implicits._
import org.joda.time.DateTime

import scala.util.{Failure, Success}
import scala.concurrent.duration._


object NetworkSync {
  def props(ourGuid:String, seeds:Seq[String], appDbRef:ActorRef, p2pFactory:P2pFactory = RealP2pFactory) =
    Props(new NetworkSync(ourGuid, seeds, appDbRef, p2pFactory))

  object Messages {
    case class AdvertiseRootHash(hash:Hash) extends P2p.P2pMessagePayload
    case class RequestApps(hash:Hash) extends P2p.P2pMessagePayload
    case class PushApps(apps:Map[String, Hash]) extends P2p.P2pMessagePayload
    case class RequestInstancesMeta(appIds:Set[String]) extends P2p.P2pMessagePayload
    case class PushInstancesMeta(apps:Map[String, Map[String, VClock]]) extends P2p.P2pMessagePayload
    case class RequestFullInstance(appId:String, instanceGuid:String) extends P2p.P2pMessagePayload
    case class PushFullInstance(instanceGuid:String,
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


class NetworkSync(ourGuid:String, seeds:Seq[String], appDbRef:ActorRef,
                  p2pFactory: P2pFactory) extends Actor {
  import NetworkSync._
  private val log =  Logging(context.system, this)
  private val p2p = p2pFactory.spawnP2p(context, ourGuid, appDbRef, seeds, "p2p")
  val appDbProxy = new AppDbProxy(appDbRef)
  override def preStart(): Unit = p2p ! P2p.Messages.Subscribe(context.self)
  import context.dispatcher

  var advertiseRootTask:Option[Cancellable] = None
  def rescheduleAdvert() = {
    advertiseRootTask.map(_.cancel())
    advertiseRootTask = Some(context.system.scheduler.schedule(1 seconds, 1 seconds,
      context.self, Tasks.AdvertiseRootTask()))
  }
  rescheduleAdvert()

  override def receive: Receive = {
    case Tasks.AdvertiseRootTask() =>
      appDbProxy.queryRootHash().map(ourHash => {
        log.debug("advertising root hash")
        p2p ! P2p.Messages.Broadcast(Messages.AdvertiseRootHash(ourHash))
      })

    case P2p.NetMessages.Message(sender, payload) => payload match {
      case Messages.AdvertiseRootHash(theirHash) =>
        appDbProxy.queryRootHash().map(ourHash => {
          if (ourHash != theirHash) {
            log.debug("root hash mismatch, start sync")
            p2p ! P2p.Messages.Send(sender, Messages.RequestApps(theirHash))
          }
        })

      case Messages.RequestApps(prevHash) =>
        appDbProxy.queryApps().map(apps => {
          appDbProxy.queryRootHash().map(ourHash => {
            if (ourHash == prevHash) {
              log.debug("syncing apps")
              p2p ! P2p.Messages.Send(sender, Messages.PushApps(apps))
            }
          })
        })

      case Messages.PushApps(theirApps) =>
        //rescheduleAdvert()
        appDbProxy.queryApps().map(ourApps => {
          val our = ourApps.toSet
          val their = theirApps.toSet
          val toSync = their.diff(our)
          if (toSync.nonEmpty) {
            log.debug(s"syncing app $toSync")
            p2p ! P2p.Messages.Send(sender, Messages.RequestInstancesMeta(toSync.map(_._1)))
          } else {
            log.debug(s"syncing nothing to $sender")
          }
        })

      case Messages.RequestInstancesMeta(appIds) =>
        log.debug(s"$sender requested instances meta")
        //rescheduleAdvert()
        appIds.map(appId => appDbProxy.queryApp(appId, stripped = false).map(r => (appId, r))).toList.sequenceU.map(res => {
          log.debug(s"sending instances meta ${res.toMap}")
          p2p ! P2p.Messages.Send(sender, Messages.PushInstancesMeta(res.toMap))
        })

      case Messages.PushInstancesMeta(theirApps) =>
        //rescheduleAdvert()
        log.debug(s"got pushInstances")
        theirApps.keys.foreach(appId => appDbProxy.queryApp(appId, stripped = false).map(ourInstances => {
          val theirInstances = theirApps(appId).toSet
          val toSync = theirInstances.diff(ourInstances.toSet)
          log.debug(s"intsances to sync $toSync, ours = ${ourInstances.toSet}, theirs=${theirInstances.toSet}")
          toSync
            .foreach(s => {
            val ourInstanceOpt = ourInstances.get(s._1)
            if (ourInstanceOpt.exists(ourVc => ourVc.isConflicting(s._2))) {
              if (ourGuid < sender) {
                // we are the resolver, let's have full instance
                log.debug(s"we($ourGuid) are the resolver for $appId, ${s._1}")
                p2p ! P2p.Messages.Send(sender, Messages.RequestFullInstance(appId, s._1))
              } else {
                log.debug(s"sender $sender is the the resolver for $appId, ${s._1}")
              }
            } else  if (ourInstanceOpt.exists(ourVc => s._2.isSubclockOf(ourVc))) {
              log.debug(s"do not request full instance, ours is newer $appId::${s._1}")
            } else {
              log.debug(s"our instance is older, requesting full instance $appId::${s._1}")
              p2p ! P2p.Messages.Send(sender, Messages.RequestFullInstance(appId, s._1))
            }
          })
        }))

      case Messages.RequestFullInstance(appId, instanceGuid) =>
        //rescheduleAdvert()
        log.debug(s"got request for full instance $appId::$instanceGuid")
        appDbProxy.queryInstance(appId, instanceGuid, stripped = false).andThen({
          case Success(AppDb.Messages.QueryInstance.Success(meta, data)) =>
            p2p ! P2p.Messages.Send(sender, Messages.PushFullInstance(instanceGuid, meta, data))
          case Success(AppDb.Messages.QueryInstance.NotExists()) =>
            log.warning("requested non existing instance, strange")
        })

      case Messages.PushFullInstance(instanceGuid, meta, data) =>
        //rescheduleAdvert()
        log.debug(s"got full instance for ${meta.appId} and $instanceGuid with meta $meta")
        appDbProxy.syncInstance(instanceGuid, meta, data).onComplete({
          case Success(AppDb.Messages.SyncInstance.Response(oldMeta, newMeta, newData)) =>
            if (oldMeta.exists(om => om.vclock.isConflicting(meta.vclock)))
              p2p ! P2p.Messages.Send(sender, Messages.PushFullInstance(instanceGuid, newMeta, newData))
          case Failure(res) =>
            log.error(s"failure during asking for instance sync $res")
        })
    }
  }
}
