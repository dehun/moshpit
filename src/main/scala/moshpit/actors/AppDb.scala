package moshpit.actors

import akka.actor.{Actor, ActorRef, Props}
import akka.event.Logging
import akka.pattern._
import akka.util.Timeout
import com.roundeights.hasher.Hash
import moshpit.{Hashable, VClock}
import com.github.nscala_time.time.Imports.DateTime
import moshpit.actors.AppDb.Tasks

import scala.concurrent.Future
import scala.concurrent.duration._
import moshpit.Hashable._
import org.joda.time.DateTimeZone

object InstanceMetaInfo {
  implicit def instanceMetaInfo2Hashable(meta:InstanceMetaInfo):Hashable = new Hashable {
    override def hash: Hash = List[Hashable](meta.vclock, meta.appId, meta.wasDeleted, meta.lastUpdated).hash
  }
}

case class InstanceMetaInfo(vclock: VClock, lastUpdated:DateTime, wasDeleted:Boolean, instanceTtlSec:Int, appId:String) {
  def update(requester:String, now:DateTime):InstanceMetaInfo =
    InstanceMetaInfo(vclock.update(requester), now, wasDeleted=false, instanceTtlSec, appId)
  def delete(requester:String, now:DateTime):InstanceMetaInfo =
    InstanceMetaInfo(vclock.update(requester), now, wasDeleted=true, instanceTtlSec, appId)
  def isDead():Boolean =
    wasDeleted || (lastUpdated.compareTo(DateTime.now(DateTimeZone.UTC).minus(instanceTtlSec*1000)) == -1)
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
  def queryInstance(appId:String, instanceGuid:String, stripped:Boolean):Future[Messages.QueryInstance.Response] =
    ask(appDb, Messages.QueryInstance.Request(appId, instanceGuid, stripped)).mapTo[Messages.QueryInstance.Response]
  def updateInstance(appId:String, instanceGuid:String, instanceData:String):Unit =
    appDb ! Messages.UpdateInstance(appId, instanceGuid, instanceData)
  def pingInstance(appId:String, instanceGuid:String):Future[Messages.PingInstance.Response] =
    ask(appDb, Messages.PingInstance.Request(appId, instanceGuid)).mapTo[Messages.PingInstance.Response]
  def syncInstance(instanceGuid:String, meta:InstanceMetaInfo, data:String):Future[Messages.SyncInstance.Response] =
    ask(appDb, Messages.SyncInstance.Request(instanceGuid, meta, data)).mapTo[Messages.SyncInstance.Response]
  def deleteInstance(appId:String, instanceGuid:String):Future[Messages.DeleteInstance.Response] =
    ask(appDb, Messages.DeleteInstance.Request(appId, instanceGuid)).mapTo[Messages.DeleteInstance.Response]
  def forceGc() =
    appDb ! Tasks.RunGc()
}

object AppDb {
  def props(ourGuid:String, intsanceTtlSec:Int, gcInstanceTtlSec:Int, gcIntervalSec:Int):Props =
    Props(new AppDb(ourGuid, gcInstanceTtlSec, gcInstanceTtlSec, gcIntervalSec))

  object Messages {
    case class UpdateInstance(appId:String, instanceGuid:String, instanceData:String)

    object SyncInstance {
      case class Request(instanceGuid: String, meta: InstanceMetaInfo, data: String)
      case class Response(oldMeta:Option[InstanceMetaInfo], newMeta:InstanceMetaInfo, newData:String)
    }

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
      case class Request(appId: String, instanceGuid: String, stripped:Boolean)
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

class HashedMap[K, V](val assocs:Map[K, V])(implicit cmp:Ordering[K],
                                            toHashableK: K => Hashable, toHashableB: V => Hashable) {
  lazy val hash = assocs.hash
}

object HashedMap {
  implicit def map2HashedMap[K, V](m:Map[K, V])(implicit cmp:Ordering[K],
                                                toHashableK: K => Hashable, toHashableB: V => Hashable):HashedMap[K, V] = new HashedMap(m)
  implicit def hashedMap2Map[K, V](m:HashedMap[K, V])(implicit cmp:Ordering[K]):Map[K, V] = m.assocs
}

class AppDb(ourGuid:String, instanceTtlSec:Int, gcInstanceTtlSec:Int, gcIntervalSec:Int) extends Actor {
  import AppDb._
  import HashedMap._
  import InstanceMetaInfo._
  import Hashable._

  private val log =  Logging(context.system, this)

  private var apps = Map.empty[String, Set[String]] // appId -> Set[InstanceGuid]
  private var instances = new HashedMap(Map.empty[(String, String), (InstanceMetaInfo, String)]) // instanceGuid -> (metainfo, data)

  import context.dispatcher
  context.system.scheduler.schedule(gcIntervalSec.seconds, gcIntervalSec.seconds,
    context.self, Tasks.RunGc())

  override def receive: Receive = {
    case Messages.UpdateInstance(appId:String, instanceGuid:String, instanceData:String) =>
      instances.get((appId, instanceGuid)) match {
        case Some((meta, _)) =>
          log.debug(s"updating old instance $appId::$instanceGuid")
          val newMetaInfo = meta.update(ourGuid, DateTime.now(DateTimeZone.UTC))
          instances = instances.updated((appId, instanceGuid), (newMetaInfo, instanceData))
          apps = apps.updated(appId, apps.get(appId).map(_ + instanceGuid).getOrElse(Set(instanceGuid)))

        case None =>
          log.debug(s"registering new instance $appId::$instanceGuid")
          apps = apps.updated(appId, apps.get(appId).map(_ + instanceGuid).getOrElse(Set(instanceGuid)))
          val newMetaInfo = InstanceMetaInfo(VClock.empty.update(ourGuid), DateTime.now(DateTimeZone.UTC),
            wasDeleted = false, instanceTtlSec, appId)
          instances = instances.updated((appId, instanceGuid), (newMetaInfo, instanceData))
      }

    case Messages.SyncInstance.Request(instanceGuid, theirMeta, theirData) =>
      instances.get((theirMeta.appId, instanceGuid)) match {
        case None =>
          log.debug(s"obtained new instance ${theirMeta.appId}::$instanceGuid")
          instances = instances.updated((theirMeta.appId, instanceGuid), (theirMeta, theirData))
          apps = apps.updated(theirMeta.appId, apps.get(theirMeta.appId).map(_ + instanceGuid).getOrElse(Set(instanceGuid)))
          sender() ! Messages.SyncInstance.Response(None, theirMeta, theirData)
        case Some((ourMeta, ourData)) =>
          if (ourMeta.vclock.isSubclockOf(theirMeta.vclock)) {
            assert (ourMeta.appId == theirMeta.appId)
            log.debug(s"substituting ours ${ourMeta.appId}::$instanceGuid with theirs ${theirMeta.appId}::$instanceGuid , as ours is part of timeline and is before")
            instances = instances.updated((theirMeta.appId, instanceGuid), (theirMeta, theirData))
            sender() ! Messages.SyncInstance.Response(Some(ourMeta), theirMeta, theirData)
          } else if (theirMeta.vclock.isSubclockOf(ourMeta.vclock)) {
            log.debug(s"ignoring update of ${theirMeta.appId}::$instanceGuid as ours is more advance")
            sender() ! Messages.SyncInstance.Response(Some(ourMeta), ourMeta, ourData)
          } else { // conflict, newest wins
            val (newMeta, newData) =
              if (theirMeta.lastUpdated.compareTo(ourMeta.lastUpdated) > 0) {
                log.debug(s"got conflicting entries for ${theirMeta.appId}::$instanceGuid, and their is newer")
                (InstanceMetaInfo(VClock.resolve(ourGuid, theirMeta.vclock, ourMeta.vclock),
                  DateTime.now(DateTimeZone.UTC), wasDeleted = theirMeta.wasDeleted,
                  instanceTtlSec, theirMeta.appId), theirData)
            } else {
                log.debug(s"got conflicting entries for ${theirMeta.appId}::$instanceGuid, and our is newer")
                (InstanceMetaInfo(VClock.resolve(ourGuid, theirMeta.vclock, ourMeta.vclock),
                DateTime.now(DateTimeZone.UTC), wasDeleted = ourMeta.wasDeleted,
                  instanceTtlSec, ourMeta.appId), ourData)
            }

            instances = instances.updated((theirMeta.appId, instanceGuid), (newMeta, newData))
            sender() ! Messages.SyncInstance.Response(Some(ourMeta), newMeta, newData)
          }
      }

    case Messages.PingInstance.Request(appId, instanceGuid) =>
      instances.get((appId, instanceGuid)) match {
        case None =>
          log.warning(s"ping of non existing instance $appId::$instanceGuid")
          sender() ! Messages.PingInstance.NotExists()
        case Some((meta, data)) =>
          if (!meta.wasDeleted) {
            log.debug(s"pinging instance $appId:$instanceGuid")
            val newMeta = meta.update(ourGuid, DateTime.now(DateTimeZone.UTC))
            instances = instances.updated((appId, instanceGuid), (newMeta, data))
            sender() ! Messages.PingInstance.Success()
          } else {
            log.warning(s"ping of dead instance $appId::$instanceGuid")
            sender() ! Messages.PingInstance.NotExists()
          }
      }

    case Messages.QueryRootHash.Request() =>
      val rootHash = instances.hash
      sender() ! Messages.QueryRootHash.Response(rootHash)

    case Messages.QueryApps.Request() =>
      val appHashes = apps.map({case (appId, instancesGuids) => {
        val appIntsances = instancesGuids.map(guid => instances((appId, guid)))
        (appId, appIntsances.hash)
      }})
      sender ! Messages.QueryApps.Response(appHashes)

    case Messages.QueryApp.Request(appId, stripped) =>
      log.debug(s"querying $appId, stripped=$stripped")
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

    case Messages.QueryInstance.Request(appId, instanceGuid, stripped) =>
      instances.get((appId, instanceGuid)).filterNot({case (meta, data) => stripped && meta.isDead() }) match {
        case None =>
          log.warning(s"queried non existing or dead instance $appId::$instanceGuid")
          sender() ! Messages.QueryInstance.NotExists()
        case Some((meta, data)) =>
          log.debug(s"successfully queried instance $appId::$instanceGuid")
          sender() ! Messages.QueryInstance.Success(meta, data)
      }

    case Messages.DeleteInstance.Request(appId, instanceGuid) =>
      instances.get((appId, instanceGuid)) match {
        case Some((meta, data)) =>
          log.debug(s"deleting $appId::$instanceGuid")
          instances = instances.updated((appId, instanceGuid), (meta.delete(ourGuid, DateTime.now(DateTimeZone.UTC)), data))
          sender() ! Messages.DeleteInstance.Success()
        case None =>
          log.info(s"deleting  error, not found $appId::$instanceGuid")
          sender() ! Messages.DeleteInstance.NotFound()
      }

    case Tasks.RunGc() =>
      log.debug("running gc")
      val instancesToClear = instances
        .filter({ case (guid, (meta, _)) =>
          meta.isDead() && meta.lastUpdated.compareTo(DateTime.now(DateTimeZone.UTC).minus(gcInstanceTtlSec*1000)) == -1 })
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
