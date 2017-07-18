package moshpit.actors

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpResponse, Multipart}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import akka.stream.ActorMaterializer
import spray.json._

import scala.io.StdIn
import scala.io.StdIn

//import akka.actor.{Actor, ActorRef, ActorSystem, Props}
//import akka.http.scaladsl.Http
//import akka.http.scaladsl.model._
//import akka.http.scaladsl.server.Directives._
//import akka.http.scaladsl.server.Route
//import akka.http.scaladsl.server.Directives._
//import akka.stream.ActorMaterializer
//import akka.stream.ActorMaterializer


object RestApi {
  def props(bindHost:String, bindPort:Int, appDbRef:ActorRef):Props =
    Props(new RestApi(bindHost, bindPort, appDbRef))
}


class RestApi(bindHost:String, bindPort:Int, appDbRef:ActorRef) extends Actor {
  val appDbProxy = new AppDbProxy(appDbRef)
  private val log =  Logging(context.system, this)
  import context.dispatcher

  val route =
    path("apps") {
      get {
        complete {
          log.info("getting apps")
          appDbProxy.queryApps().map(_.keySet.toString())
        }
      }
    } ~
    path("app" / "[a-zA-Z0-9_-]+".r) { appId =>
      get {
        complete {
          log.info(s"querying app $appId")
          appDbProxy.queryApp(appId).map(instances => {
            instances.keySet.toString
          })
        }
      }
    } ~
    path ("app" / "[a-zA-Z0-9_-]+".r / "instance" / "[a-zA-Z0-9_-]+".r) { (appId, instanceGuid) =>
      get {
        log.info(s"getting instance $appId::$instanceGuid")
        complete {
          appDbProxy.queryInstance(appId, instanceGuid).map({
            case AppDb.Messages.QueryInstance.Success(meta, data) =>
              HttpResponse(200,entity=(appId, instanceGuid, meta, data).toString())
            case AppDb.Messages.QueryInstance.NotExists() =>
              HttpResponse(404, entity="instance not found")
          })
        }
      } ~
      put {
        entity(as[String]) { data =>
          log.info(s"putting instance $appId::$instanceGuid")
          appDbProxy.updateInstance(appId, instanceGuid, data)
          complete(s"updated $appId::$instanceGuid with $data")
        }
      } ~
      patch {
        log.info(s"pinging $appId::$instanceGuid")
        complete {
          appDbProxy.pingInstance(appId, instanceGuid).map({
            case AppDb.Messages.PingInstance.NotExists() =>
              HttpResponse(404, entity="instance not found")
            case AppDb.Messages.PingInstance.Success() =>
              HttpResponse(200, entity="pinged")
          })
        }
      }

    }


  implicit val materializer = ActorMaterializer()
  val bindingFuture = Http()(context.system).bindAndHandle(RouteResult.route2HandlerFlow(route), bindHost, bindPort)

  override def receive: Receive = {case _ => {}}
}
