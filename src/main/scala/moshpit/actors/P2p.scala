package moshpit.actors

import akka.actor.{Actor, ActorContext, ActorPath, ActorRef, Props, Terminated}
import akka.event.Logging
import akka.pattern.ask

import scala.concurrent.duration._
import scala.util.{Failure, Success}
import cats._
import cats.implicits._
import cats.data._

object P2p {
  def props(guid:String, appDbRef:ActorRef, seeds:Seq[String]) = Props(new P2p(guid, appDbRef, seeds))

  trait P2pMessagePayload

  object NetMessages { // from network
    case class Hello(guid: String, sender:ActorRef)
    case class HelloAck(guid:String)
    case class Message(sender:String, payload: P2pMessagePayload)
  }

  object Messages {  // from our system (TODO: separate maybe?)
    case class Broadcast(payload:P2pMessagePayload)
    case class Send(guid:String, payload:P2pMessagePayload)
    case class Subscribe(subscriber:ActorRef)
    case class ConnectedPeer(guid:String, ref:ActorRef)
  }

  object Tasks {
    case class ReconnectPeers()
  }
}

class P2p(ourGuid:String, appDbRef:ActorRef, seeds:Seq[String]) extends Actor {
  private val log =  Logging(context.system, this)
  log.info("p2p starting up with seeds")

  private var peers = Map.empty[String, ActorRef]
  private val appDbProxy = new AppDbProxy(appDbRef)
  private var subscribers = Set.empty[ActorRef]
  import context.dispatcher

  context.system.scheduler.schedule(2 seconds, 2 second,
    context.self, P2p.Tasks.ReconnectPeers())

  def hostportToActorPath(s:String) = ActorPath.fromString(s"akka.tcp://Main@$s/user/app/networkSync/p2p")

  val ourPath = hostportToActorPath(
    context.system.settings.config.getString("akka.remote.netty.tcp.hostname")+ ":" +
      context.system.settings.config.getInt("akka.remote.netty.tcp.port").toString)

  appDbProxy.updateInstance("moshpit", ourGuid, ourPath.toString)
  context.system.scheduler.schedule(15 seconds, 15 seconds,
    () => appDbProxy.pingInstance("moshpit", ourGuid))

  override def receive: Receive = {
    case P2p.Messages.ConnectedPeer(guid, ref) =>
      if (guid != ourGuid) {
        log.info(s"got new peer with guid $guid and path ${ref.path}")
        context.watch(ref)
        peers = peers.updated(guid, ref)
      }
    case P2p.NetMessages.Hello(guid, hisender) =>
      log.info(s"hoi from ${guid}")
      context.self ! P2p.Messages.ConnectedPeer(guid, hisender)
      sender ! P2p.NetMessages.HelloAck(ourGuid)
    case payloadMsg:P2p.NetMessages.Message => subscribers.foreach(_ ! payloadMsg)
    case P2p.Messages.Subscribe(subscriber) => subscribers = subscribers + subscriber
    case P2p.Messages.Broadcast(payload) => peers.foreach(_._2 ! P2p.NetMessages.Message(ourGuid, payload)) // wtf?
    case P2p.Messages.Send(guid, payload) => peers.get(guid).foreach(_ ! P2p.NetMessages.Message(ourGuid, payload))
    case P2p.Tasks.ReconnectPeers() =>
      val me = context.self
      val connectedPaths = peers.values.map(_.path).toSet + ourPath
      for {
        instanceGuids <- appDbProxy.queryApp("moshpit", stripped = false).map(_.keySet)
        instances <- instanceGuids.map(guid => appDbProxy.queryInstance("moshpit", guid, stripped=false)).toList.sequenceU
      } {
        val knownPaths = instances.filter(_.isInstanceOf[AppDb.Messages.QueryInstance.Success])
          .map({case AppDb.Messages.QueryInstance.Success(_, data) => ActorPath.fromString(data)}).toSet ++
          seeds.map(hostportToActorPath) - ourPath
        val toReconnect = knownPaths.diff(connectedPaths)
        toReconnect.foreach(path =>
          context.actorSelection(path).resolveOne(5 seconds).andThen({
            case Success(ref: ActorRef) =>
              log.info(s"resolved $path, asking")
              ask(ref, P2p.NetMessages.Hello(ourGuid, me))(5 seconds).mapTo[P2p.NetMessages.HelloAck]
                .andThen({
                  case Success(hi) =>
                    log.info(s"reconnected $path")
                    me ! P2p.Messages.ConnectedPeer(hi.guid, ref)
                  case Failure(err) => log.warning(s"failed to ask hello $path")
                })
            case Failure(err) => log.warning(s"failed to resolve address $path")
          }))
      }
    case Terminated(ref) => peers.find(_._2 == ref).map(p => peers = peers - p._1)
  }
}

trait P2pFactory {
  def spawnP2p(context:ActorContext, ourGuid:String, appDbRef:ActorRef, seeds:Seq[String], actorName:String):ActorRef
}

object RealP2pFactory extends P2pFactory {
  override def spawnP2p(context: ActorContext, ourGuid: String, appDbRef: ActorRef, seeds: Seq[String], actorName:String): ActorRef =
    context.actorOf(P2p.props(ourGuid, appDbRef, seeds), actorName)
}