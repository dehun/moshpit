package moshpit.actors

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpResponse, Multipart}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import akka.stream.ActorMaterializer
import com.github.nscala_time.time.Imports.DateTime
import moshpit.VClock
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.marshalling.Marshal
import org.joda.time.format.ISODateTimeFormat
import spray.json._

object RestApi {
  def props(bindHost:String, bindPort:Int, appDbRef:ActorRef):Props =
    Props(new RestApi(bindHost, bindPort, appDbRef))
}

// json responses
final case class SuccessRes(note:String)
final case class FailureRes(reason:String)
final case class Instance(appId:String, instanceGuid:String, lastUpdated:DateTime, data:String)
final case class App(instances:Set[String])
final case class Apps(ids:Set[String])

trait JsonSupport extends SprayJsonSupport with DefaultJsonProtocol {
  implicit object DateTimeFormat extends RootJsonFormat[DateTime] {
    val formatter = ISODateTimeFormat.basicDateTimeNoMillis
    def write(obj: DateTime): JsValue = {
      JsString(formatter.print(obj))
    }
    def read(json: JsValue): DateTime = json match {
      case JsString(s) => try {
        formatter.parseDateTime(s)
      }
      catch {
        case t: Throwable => error(s)
      }
      case _ =>
        error(json.toString())
    }

    def error(v: Any): DateTime = {
      val example = formatter.print(0)
      deserializationError(f"'$v' is not a valid date value. Dates must be in compact ISO-8601 format, e.g. '$example'")
    }
  }

  implicit val successFormat = jsonFormat1(SuccessRes)
  implicit val failureFormat = jsonFormat1(FailureRes)
  implicit val instanceFormat = jsonFormat4(Instance)
  implicit val appFormat = jsonFormat1(App)
  implicit val appsFormat = jsonFormat1(Apps)
}


class RestApi(bindHost:String, bindPort:Int, appDbRef:ActorRef) extends Actor with JsonSupport {
  val appDbProxy = new AppDbProxy(appDbRef)
  private val log =  Logging(context.system, this)
  import context.dispatcher

  val route =
    path("apps") {
      get {
        complete {
          log.info("getting apps")
          appDbProxy.queryApps().map(r => Apps(r.keySet))
        }
      }
    } ~
    path("app" / "[a-zA-Z0-9_-]+".r) { appId =>
      get {
        complete {
          log.info(s"querying app $appId")
          appDbProxy.queryApp(appId, stripped = true).map(instances => {
            App(instances.keySet)
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
              HttpResponse(200, entity=Instance(appId, instanceGuid, meta.lastUpdated, data).toJson.toString())
            case AppDb.Messages.QueryInstance.NotExists() =>
              HttpResponse(404, entity=FailureRes("instance not found").toJson.toString())
          })
        }
      } ~
      put {
        entity(as[String]) { data =>
          log.info(s"putting instance $appId::$instanceGuid")
          appDbProxy.updateInstance(appId, instanceGuid, data)
          complete(SuccessRes(s"updated $appId::$instanceGuid"))
        }
      } ~
      patch {
        log.info(s"pinging $appId::$instanceGuid")
        complete {
          appDbProxy.pingInstance(appId, instanceGuid).map({
            case AppDb.Messages.PingInstance.NotExists() =>
              HttpResponse(404, entity=FailureRes("instance not found").toJson.toString())
            case AppDb.Messages.PingInstance.Success() =>
              HttpResponse(200, entity=SuccessRes("pinged").toJson.toString())
          })
        }
      } ~
      delete {
        log.info("deleting $appId::$instanceGuid")
        complete {
          appDbProxy.deleteInstance(appId, instanceGuid).map({
            case AppDb.Messages.DeleteInstance.Success() =>
              HttpResponse(200, entity=SuccessRes("instance was successfully deleted").toJson.toString)
            case AppDb.Messages.DeleteInstance.NotFound() =>
              HttpResponse(404, entity=FailureRes("instance was not found").toJson.toString)
          })
        }
      }
    }


  implicit val materializer = ActorMaterializer()
  val bindingFuture = Http()(context.system).bindAndHandle(RouteResult.route2HandlerFlow(route), bindHost, bindPort)

  override def receive: Receive = {case _ => {}}
}
