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
  def queryInstance(appId:String, instanceGuid:String):Future[Messages.QueryInstance.Response] =
    ask(appDb, Messages.QueryInstance.Request(appId, instanceGuid)).mapTo[Messages.QueryInstance.Response]
  def updateInstance(appId:String, instanceGuid:String, instanceData:String):Unit =
    appDb ! Messages.UpdateInstance(appId, instanceGuid, instanceData)
  def pingInstance(appId:String, instanceGuid:String):Future[Messages.PingInstance.Response] =
    ask(appDb, Messages.PingInstance.Request(appId, instanceGuid)).mapTo[Messages.PingInstance.Response]
  def syncInstance(appId:String, instanceGuid:String, meta:InstanceMetaInfo, data:String):Unit =
    appDb ! Messages.SyncInstance(appId, instanceGuid, meta, data)
  def deleteInstance(appId:String, instanceGuid:String):Future[Messages.DeleteInstance.Response] =
    ask(appDb, Messages.DeleteInstance.Request(appId, instanceGuid)).mapTo[Messages.DeleteInstance.Response]
}

object AppDb {
  def props(ourGuid:String, intsanceTtlSec:Int, gcInstanceTtlSec:Int, gcIntervalSec:Int):Props =
    Props(new AppDb(ourGuid, gcInstanceTtlSec, gcInstanceTtlSec, gcIntervalSec))

  object Messages {
    case class UpdateInstance(appId:String, instanceGuid:String, instanceData:String)
    case class SyncInstance(appId:String, instanceGuid:String, meta:InstanceMetaInfo, data:String)
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
  private var instances = Map.empty[String, (InstanceMetaInfo, String)] // instanceGuid -> (metainfo, data)

  import AppDb._
  import context.dispatcher
  context.system.scheduler.schedule(gcIntervalSec.seconds, gcIntervalSec.seconds,
    context.self, Tasks.RunGc())

  override def receive: Receive = {
    case Messages.UpdateInstance(appId:String, instanceGuid:String, instanceData:String) =>
      instances.get(instanceGuid).filter(_._1.appId == appId) match {
        case Some((meta, _)) =>
          log.info(s"updating old instance $appId::$instanceGuid")
          val newMetaInfo = meta.update(ourGuid, DateTime.now())
          instances = instances.updated(instanceGuid, (newMetaInfo, instanceData))
        case None =>
          log.info(s"registering new instance $appId::$instanceGuid")
          apps = apps.updated(appId, apps.get(appId).map(_ + instanceGuid).getOrElse(Set(instanceGuid)))
          val newMetaInfo = InstanceMetaInfo(VClock.empty.update(ourGuid), DateTime.now(),
            wasDeleted = false, instanceTtlSec, appId)
          instances = instances.updated(instanceGuid, (newMetaInfo, instanceData))
      }

    case Messages.SyncInstance(appId, instanceGuid, theirMeta, theirData) =>
      instances.get(instanceGuid) match {
        case None =>
          log.info(s"obtained new instance $appId::$instanceGuid")
          instances = instances.updated(instanceGuid, (theirMeta, theirData))
          apps = apps.updated(appId, apps.get(appId).map(_ + instanceGuid).getOrElse(Set(instanceGuid)))
        case Some((ourMeta, ourData)) =>
          if (ourMeta.vclock.isSubclockOf(theirMeta.vclock)) {
            log.info(s"substituting ours $appId::$instanceGuid with theirs, as ours is part of timeline and is before")
            instances = instances.updated(instanceGuid, (theirMeta, theirData))
          } else if (theirMeta.vclock.isSubclockOf(ourMeta.vclock)) {
            log.info(s"ignoring update of $appId::$instanceGuid as ours is more advance")
          } else { // conflict, newest wins
            val newMeta = InstanceMetaInfo(VClock.resolve(theirMeta.vclock, ourMeta.vclock),
              DateTime.now(), wasDeleted = false, instanceTtlSec, appId)
            if (theirMeta.lastUpdated.compareTo(ourMeta.lastUpdated) > 0) {
              log.info(s"got conflicting entries for $appId::$instanceGuid, and their is newer")
              instances = instances.updated(instanceGuid, (newMeta, theirData))
            } else {
              log.info(s"got conflicting entries for $appId::$instanceGuid, and our is newer")
              instances = instances.updated(instanceGuid, (newMeta, ourData))
            }
          }
      }

    case Messages.PingInstance.Request(appId, instanceGuid) =>
      instances.get(instanceGuid) match {
        case None =>
          log.warning(s"ping of non existing instance $appId::$instanceGuid")
          sender() ! Messages.PingInstance.NotExists()
        case Some((meta, data)) =>
          if (!meta.wasDeleted) {
            log.info(s"pinging instance $appId:$instanceGuid")
            val newMeta = meta.update(ourGuid, DateTime.now())
            instances = instances.updated(instanceGuid, (newMeta, data))
            sender() ! Messages.PingInstance.Success()
          } else {
            log.warning(s"ping of dead instance $appId::$instanceGuid")
            sender() ! Messages.PingInstance.NotExists()
          }
      }

    case Messages.QueryRootHash.Request() =>
      val rootHash = instances.toString().sha1.hash
      sender() ! Messages.QueryRootHash.Response(rootHash)

    case Messages.QueryApps.Request() =>
      log.info("querying apps")
      val appHashes = apps.map({case (appId, instancesGuids) => {
        val appIntsances = instancesGuids.map(instances.apply(_))
        (appId, appIntsances.toString().sha1.hash)
      }})
      sender ! Messages.QueryApps.Response(appHashes)

    case Messages.QueryApp.Request(appId, stripped) =>
      val appIntsances = apps.getOrElse(appId, Set.empty)
          .filterNot(guid => stripped && instances(guid)._1.isDead())
          .map(guid => (guid, instances(guid)._1.vclock))
      sender ! Messages.QueryApp.Response(appIntsances.toMap)

    case Messages.QueryInstance.Request(appId, instanceGuid) =>
      instances.get(instanceGuid).filter(!_._1.isDead()).filter(_._1.appId==appId) match {
        case None =>
          log.warning(s"queried non existing or dead instance $appId::$instanceGuid")
          sender() ! Messages.QueryInstance.NotExists()
        case Some((meta, data)) =>
          log.info(s"successfully queried instance $appId::$instanceGuid")
          sender() ! Messages.QueryInstance.Success(meta, data)
      }

    case Messages.DeleteInstance.Request(appId, instanceGuid) =>
      if (apps.get(appId).exists(_.contains(instanceGuid))) {
        log.info(s"deleting $appId::$instanceGuid")
        instances -= instanceGuid
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
      instancesToClear.map(guid => {
        instances = instances -- instancesToClear
        apps = apps.map({case (app, appInstances) => (app, appInstances.filterNot(i => instancesToClear.contains(i)))})
      })

    case _ =>
      log.error("unknown message received, failing")
      ???
  }
}
