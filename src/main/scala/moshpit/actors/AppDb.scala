package moshpit.actors

import akka.actor.{Actor, ActorRef, Props}
import akka.event.Logging
import akka.pattern._
import akka.util.Timeout
import com.roundeights.hasher.Hash
import com.roundeights.hasher.Implicits._
import moshpit.VClock
import com.github.nscala_time.time.Imports.DateTime

import scala.concurrent.Future
import scala.concurrent.duration._

case class InstanceMetaInfo(vclock: VClock, lastUpdated:DateTime, wasDeleted:Boolean, instanceTtlSec:Int, appId:String) {
  def update(requester:String, now:DateTime):InstanceMetaInfo =
    InstanceMetaInfo(vclock.update(requester), now, wasDeleted=false, instanceTtlSec, appId)
  def delete(requester:String, now:DateTime):InstanceMetaInfo =
    InstanceMetaInfo(vclock.update(requester), now, wasDeleted=true, instanceTtlSec, appId)
  def isDead():Boolean =
    wasDeleted || (lastUpdated.compareTo(DateTime.now().minus(instanceTtlSec*1000)) == -1)
}

class AppDbProxy(appDb:ActorRef) {
  import scala.concurrent.ExecutionContext.Implicits.global
  import AppDb.Messages
  implicit val timeout = Timeout(5 seconds)
  def queryRootHash():Future[Hash] =
    ask(appDb, Messages.QueryRootHash.Request()).mapTo[Messages.QueryRootHash.Response].map(_.hash)
  def queryApps():Future[Map[String, Hash]] =
    ask(appDb, Messages.QueryApps.Request()).mapTo[Messages.QueryApps.Response].map(_.apps)
  def queryApp(appId:String, stripped:Boolean):Future[Map[String, VClock]] =
    ask(appDb, Messages.QueryApp.Request(appId, stripped)).mapTo[Messages.QueryApp.Response].map(_.instances)
  def queryFullApp(appId:String, stripped:Boolean):Future[Map[String, (InstanceMetaInfo, String)]] =
    ask(appDb, Messages.QueryFullApp.Request(appId, stripped)).mapTo[Messages.QueryFullApp.Response].map(_.instances)
  def queryInstance(appId:String, instanceGuid:String):Future[Messages.QueryInstance.Response] =
    ask(appDb, Messages.QueryInstance.Request(appId, instanceGuid)).mapTo[Messages.QueryInstance.Response]
  def updateInstance(appId:String, instanceGuid:String, instanceData:String):Unit =
    appDb ! Messages.UpdateInstance(appId, instanceGuid, instanceData)
  def pingInstance(appId:String, instanceGuid:String):Future[Messages.PingInstance.Response] =
    ask(appDb, Messages.PingInstance.Request(appId, instanceGuid)).mapTo[Messages.PingInstance.Response]
  def syncInstance(appId:String, instanceGuid:String, meta:InstanceMetaInfo, data:String):Unit =
    appDb ! Messages.SyncInstance(instanceGuid, meta, data)
  def deleteInstance(appId:String, instanceGuid:String):Future[Messages.DeleteInstance.Response] =
    ask(appDb, Messages.DeleteInstance.Request(appId, instanceGuid)).mapTo[Messages.DeleteInstance.Response]
}

object AppDb {
  def props(ourGuid:String, intsanceTtlSec:Int, gcInstanceTtlSec:Int, gcIntervalSec:Int):Props =
    Props(new AppDb(ourGuid, gcInstanceTtlSec, gcInstanceTtlSec, gcIntervalSec))

  object Messages {
    case class UpdateInstance(appId:String, instanceGuid:String, instanceData:String)
    case class SyncInstance(instanceGuid:String, meta:InstanceMetaInfo, data:String)
    object PingInstance {
      case class Request(appId: String, instanceGuid: String)
      trait Response
      case class Success() extends Response
      case class NotExists() extends Response
    }

    object QueryRootHash {
      case class Request()
      case class Response(hash:Hash)
    }

    object QueryApps {
      case class Request()
      case class Response(apps: Map[String, Hash])
    }

    object QueryApp {
      case class Request(appId: String, stripped:Boolean)
      case class Response(instances: Map[String, VClock])
    }

    object QueryFullApp {
      case class Request(appId: String, stripped:Boolean)
      case class Response(instances: Map[String, (InstanceMetaInfo, String)])
    }

    object QueryInstance {
      case class Request(appId: String, instanceGuid: String)
      trait Response
      case class NotExists() extends Response
      case class Success(metainfo:InstanceMetaInfo, data: String) extends Response
    }

    object DeleteInstance {
      case class Request(appId:String, instanceGuid:String)
      trait Response
      case class Success() extends Response
      case class NotFound() extends Response
    }
  }

  object Tasks {
    case class RunGc()
  }
}

class AppDb(ourGuid:String, instanceTtlSec:Int, gcInstanceTtlSec:Int, gcIntervalSec:Int) extends Actor {
  private val log =  Logging(context.system, this)

  private var apps = Map.empty[String, Set[String]] // appId -> Set[InstanceGuid]
  private var instances = Map.empty[(String, String), (InstanceMetaInfo, String)] // instanceGuid -> (metainfo, data)

  import AppDb._
  import context.dispatcher
  context.system.scheduler.schedule(gcIntervalSec.seconds, gcIntervalSec.seconds,
    context.self, Tasks.RunGc())

  override def receive: Receive = {
    case Messages.UpdateInstance(appId:String, instanceGuid:String, instanceData:String) =>
      instances.get((appId, instanceGuid)) match {
        case Some((meta, _)) =>
          log.info(s"updating old instance $appId::$instanceGuid")
          val newMetaInfo = meta.update(ourGuid, DateTime.now())
          instances = instances.updated((appId, instanceGuid), (newMetaInfo, instanceData))
          apps = apps.updated(appId, apps.get(appId).map(_ + instanceGuid).getOrElse(Set(instanceGuid)))

        case None =>
          log.info(s"registering new instance $appId::$instanceGuid")
          apps = apps.updated(appId, apps.get(appId).map(_ + instanceGuid).getOrElse(Set(instanceGuid)))
          val newMetaInfo = InstanceMetaInfo(VClock.empty.update(ourGuid), DateTime.now(),
            wasDeleted = false, instanceTtlSec, appId)
          instances = instances.updated((appId, instanceGuid), (newMetaInfo, instanceData))
      }

    case Messages.SyncInstance(instanceGuid, theirMeta, theirData) =>
      instances.get((theirMeta.appId, instanceGuid)) match {
        case None =>
          log.info(s"obtained new instance ${theirMeta.appId}::$instanceGuid")
          instances = instances.updated((theirMeta.appId, instanceGuid), (theirMeta, theirData))
          apps = apps.updated(theirMeta.appId, apps.get(theirMeta.appId).map(_ + instanceGuid).getOrElse(Set(instanceGuid)))
        case Some((ourMeta, ourData)) =>
          if (ourMeta.vclock.isSubclockOf(theirMeta.vclock)) {
            assert (ourMeta.appId == theirMeta.appId)
            log.info(s"substituting ours ${ourMeta.appId}::$instanceGuid with theirs ${theirMeta.appId}::$instanceGuid , as ours is part of timeline and is before")
            instances = instances.updated((theirMeta.appId, instanceGuid), (theirMeta, theirData))
          } else if (theirMeta.vclock.isSubclockOf(ourMeta.vclock)) {
            log.info(s"ignoring update of ${theirMeta.appId}::$instanceGuid as ours is more advance")
          } else { // conflict, newest wins
            val (newMeta, newData) =
              if (theirMeta.lastUpdated.compareTo(ourMeta.lastUpdated) > 0) {
                log.debug(s"got conflicting entries for ${theirMeta.appId}::$instanceGuid, and their is newer")
                (InstanceMetaInfo(VClock.resolve(theirMeta.vclock, ourMeta.vclock),
                  DateTime.now(), wasDeleted = theirMeta.wasDeleted, instanceTtlSec, theirMeta.appId), theirData)
            } else {
                log.debug(s"got conflicting entries for ${theirMeta.appId}::$instanceGuid, and our is newer")
                (InstanceMetaInfo(VClock.resolve(theirMeta.vclock, ourMeta.vclock),
                DateTime.now(), wasDeleted = ourMeta.wasDeleted, instanceTtlSec, ourMeta.appId), ourData)
            }

            instances = instances.updated((theirMeta.appId, instanceGuid), (newMeta, theirData))
          }
      }

    case Messages.PingInstance.Request(appId, instanceGuid) =>
      instances.get((appId, instanceGuid)) match {
        case None =>
          log.warning(s"ping of non existing instance $appId::$instanceGuid")
          sender() ! Messages.PingInstance.NotExists()
        case Some((meta, data)) =>
          if (!meta.wasDeleted) {
            log.info(s"pinging instance $appId:$instanceGuid")
            val newMeta = meta.update(ourGuid, DateTime.now())
            instances = instances.updated((appId, instanceGuid), (newMeta, data))
            sender() ! Messages.PingInstance.Success()
          } else {
            log.warning(s"ping of dead instance $appId::$instanceGuid")
            sender() ! Messages.PingInstance.NotExists()
          }
      }

    case Messages.QueryRootHash.Request() =>
      val rootHash = instances.toList.sortBy(_._1).toString().sha1.hash
      sender() ! Messages.QueryRootHash.Response(rootHash)

    case Messages.QueryApps.Request() =>
      log.debug("querying apps")
      val appHashes = apps.map({case (appId, instancesGuids) => {
        val appIntsances = instancesGuids.toList.sorted.map(guid => instances((appId, guid)))
        log.info("{} {}", appId, appIntsances.toString().sha1.hash)
        (appId, appIntsances.toString().sha1.hash)
      }})
      sender ! Messages.QueryApps.Response(appHashes)

    case Messages.QueryApp.Request(appId, stripped) =>
      log.info(s"querying $appId, stripped=$stripped")
      val appIntsances = apps.getOrElse(appId, Set.empty)
          .filterNot(guid => stripped && instances((appId, guid))._1.isDead())
          .map(guid => (guid, instances((appId, guid))._1.vclock))
      sender ! Messages.QueryApp.Response(appIntsances.toMap)

    case Messages.QueryFullApp.Request(appId, stripped) =>
      log.debug(s"querying full $appId, stripped=$stripped")
      val appIntsances = apps.getOrElse(appId, Set.empty)
        .filterNot(guid => stripped && instances((appId, guid))._1.isDead())
        .map(guid => (guid, instances((appId, guid)))).toMap
      sender ! Messages.QueryFullApp.Response(appIntsances)

    case Messages.QueryInstance.Request(appId, instanceGuid) =>
      instances.get((appId, instanceGuid)).filter(!_._1.isDead()).filter(_._1.appId==appId) match {
        case None =>
          log.warning(s"queried non existing or dead instance $appId::$instanceGuid")
          sender() ! Messages.QueryInstance.NotExists()
        case Some((meta, data)) =>
          log.debug(s"successfully queried instance $appId::$instanceGuid")
          sender() ! Messages.QueryInstance.Success(meta, data)
      }

    case Messages.DeleteInstance.Request(appId, instanceGuid) =>
      if (apps.get(appId).exists(_.contains(instanceGuid))) {
        log.info(s"deleting $appId::$instanceGuid")
        instances -= ((appId, instanceGuid))
        apps = apps.updated(appId, apps.get(appId).map(is => is - instanceGuid).getOrElse(Set.empty))
        sender() ! Messages.DeleteInstance.Success()
      } else {
        log.info(s"deleting  error, not found $appId::$instanceGuid")
        sender() ! Messages.DeleteInstance.NotFound()
      }

    case Tasks.RunGc() =>
      log.info("running gc")
      val instancesToClear = instances
        .filter({ case (guid, (meta, _)) =>
          meta.isDead() && meta.lastUpdated.compareTo(DateTime.now().minus(gcInstanceTtlSec*1000)) == -1 })
        .keys.toSet
      instancesToClear.foreach({case (appId, guid) =>
        apps = apps.updated(appId, apps.get(appId).map(_ - guid).getOrElse(Set.empty))
      })
      instances = instances -- instancesToClear

    case _ =>
      log.error("unknown message received, failing")
      ???
  }
}
