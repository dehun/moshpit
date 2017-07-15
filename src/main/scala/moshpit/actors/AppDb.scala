package moshpit.actors

import akka.actor.{Actor, ActorRef}

class AppDbProxy(appDb:ActorRef) {
  def register() = ???
}

object AppDbMessages {
  object Register {
    case class Request(appId:String, instanceGuid:String, instanceData:String)
    trait Response
    case class Failed(reason:String) extends Response
    case class Success() extends Response
  }
  object QueryApp {
    case class Request(appId:String)
    trait Response
    case class Success(instances:Set[String]) extends Response
  }
  object QueryInstance {
    case class Request(appId:String, instanceGuid:String)
    trait Response
    case class NotExists() extends Response
    case class Success(instanceData:String) extends Response
  }

  object QueryApps {
    case class Request()
    case class Response(appIds:Set[String])
  }
}

class AppDb extends Actor {
  override def receive: Receive = ???
}
