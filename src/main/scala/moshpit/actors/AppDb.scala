package moshpit.actors

import akka.actor.{Actor, ActorRef}
import akka.event.Logging
import akka.pattern._
import akka.util.Timeout
import com.roundeights.hasher.Digest
import moshpit.VClock
import java.util.{Calendar, Date}

import scala.concurrent.Future
import scala.concurrent.duration._

case class InstanceMetaInfo(vclock: VClock, lastUpdated:Date) {
  def update(requester:String, now:Date):InstanceMetaInfo = InstanceMetaInfo(vclock.update(requester), now)
}

class AppDbProxy(appDb:ActorRef) {
  import scala.concurrent.ExecutionContext.Implicits.global
  import AppDb.Messages
  implicit val timeout = Timeout(5 seconds)
  def queryRootHash() = ask(appDb, Messages.QueryRootHash.Request()).mapTo[Messages.QueryRootHash.Response].map(_.hash)
  def queryApps():Future[Map[String, Digest]] =
    ask(appDb, Messages.QueryApps.Request).mapTo[Messages.QueryApps.Response].map(_.apps)
  def queryApp(appId:String) = ask(appDb, Messages.QueryApp.Request(appId)).mapTo[Messages.QueryApp.Response].map(_.instances)
  def queryInstance(appId:String, instanceGuid:String) =
    ask(appDb, Messages.QueryInstance.Request(appId, instanceGuid))
  def updateInstance(appId:String, instanceGuid:String, instanceData:String):Unit =
    appDb ! Messages.UpdateInstance(appId, instanceGuid, instanceData)
  def pingInstance(appId:String, instanceGuid:String) =
    ask(appDb, Messages.PingInstance.Request(appId, instanceGuid))
  def syncInstance(appId:String, instanceGuid:String, meta:InstanceMetaInfo, data:String) =
    appDb ! Messages.SyncInstance(appId, instanceGuid, meta, data)
}

object AppDb {
  object Messages {
    case class UpdateInstance(appId:String, instanceGuid:String, instanceData:String)
    case class SyncInstance(appId:String, instanceGuid:String, meta:InstanceMetaInfo, data:String)
    object PingInstance {
      case class Request(appId: String, instanceGuid: String)
    }

    object QueryRootHash {
      case class Request()
      case class Response(hash:Digest)
    }

    object QueryApps {
      case class Request()
      case class Response(apps: Map[String, Digest])
    }

    object QueryApp {
      case class Request(appId: String)
      case class Response(instances: Map[String, VClock])
    }

    object QueryInstance {
      case class Request(appId: String, instanceGuid: String)
      trait Response
      case class NotExists() extends Response
      case class Success(metainfo:InstanceMetaInfo, data: String) extends Response
    }
  }
}

class AppDb(ourGuid:String) extends Actor {
  private val log =  Logging(context.system, this)

  private var apps = Map.empty[String, Set[String]] // appId -> Set[InstanceGuid]
  private var instances = Map.empty[String, (InstanceMetaInfo, String)] // instanceGuid -> (metainfo, data)

  import AppDb._
  override def receive: Receive = {
    case Messages.UpdateInstance(appId:String, instanceGuid:String, instanceData:String) =>
      instances.get(instanceGuid) match {
        case Some((meta, _)) =>
          log.info(s"updating old instance $appId::$instanceGuid")
          val newMetaInfo = meta.update(ourGuid, Calendar.getInstance().getTime())
          instances = instances.updated(instanceGuid, (newMetaInfo, instanceData))
        case None =>
          log.info(s"registering new instance $appId::$instanceGuid")
          apps = apps.updated(appId, apps.get(appId).map(_ + instanceGuid).getOrElse(Set(instanceGuid)))
          val newMetaInfo = InstanceMetaInfo(VClock.empty.update(ourGuid), Calendar.getInstance().getTime())
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
              Calendar.getInstance().getTime)
            if (theirMeta.lastUpdated.compareTo(ourMeta.lastUpdated) > 0) {
              log.info(s"got conflicting entries for $appId::$instanceGuid, and their is newer")
              instances = instances.updated(instanceGuid, (newMeta, theirData))
            } else {
              log.info(s"got conflicting entries for $appId::$instanceGuid, and our is newer")
              instances = instances.updated(instanceGuid, (newMeta, ourData))
            }
          }
      }
  }
}
