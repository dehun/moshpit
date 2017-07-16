package moshpit.actors

import akka.actor.{Actor, ActorRef}
import akka.event.Logging
import akka.pattern._
import akka.util.Timeout
import scala.concurrent.duration._

class AppDbProxy(appDb:ActorRef) {
  import AppDb.Messages
  implicit val timeout = Timeout(10 seconds)
  def queryApps() = ask(appDb, Messages.QueryApps.Request).mapTo[Messages.QueryApps.Response].map(_.appIds)
  def queryApp(appId:String) = ask(appDb, Messages.QueryApp.Request(appId)).mapTo[Messages.QueryApp.Response]
  def queryInstance(appId:String, instanceGuid:String) = ask(appDb, Messages.QueryInstance.Request(appId, instanceGuid))
  def register(appId:String, instanceGuid:String, instanceData:String) =
    ask(appDb, Messages.Register.Request(appId, instanceGuid, instanceData)).mapTo[Messages.Register.Response]

}

object AppDb {
  object Messages {
    object Register {
      case class Request(appId: String, instanceGuid: String, instanceData: String)
      trait Response
      case class Failed(reason: String) extends Response
      case class Success() extends Response
    }

    object QueryApp {
      case class Request(appId: String)
      trait Response
      case class Success(instances: Set[String]) extends Response
      case class NotExists()
    }

    object QueryInstance {
      case class Request(appId: String, instanceGuid: String)
      trait Response
      case class NotExists() extends Response
      case class Success(instanceData: String) extends Response
    }

    object QueryApps {
      case class Request()
      case class Response(appIds: Set[String])
    }
  }

}

class AppDb extends Actor {
  private val log =  Logging(context.system, this)

  override def receive: Receive = {
    case _ => log.info("appdb got message")
  }
}
