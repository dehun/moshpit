package moshpit.actors

import akka.actor.{Actor, ActorPath, ActorRef, Props, Terminated}
import akka.event.Logging
import akka.pattern.ask

import scala.concurrent.duration._
import scala.util.{Failure, Success}

object P2p {
  def props(guid:String, seeds:Seq[String]) = Props(new P2p(guid, seeds))

  trait P2pMessagePayload

  object NetMessages { // from network
    case class Hello(guid: String, sender:ActorRef)
    case class HelloAck(guid:String)
    case class Message(sender:String, payload: P2pMessagePayload)
    case class AnnouncePeers(peers:Set[ActorPath])
  }

  object Messages {  // from our system (TODO: separate maybe?)
    case class Broadcast(payload:P2pMessagePayload)
    case class Send(guid:String, payload:P2pMessagePayload)
    case class Subscribe(subscriber:ActorRef)
    case class ConnectedPeer(guid:String, ref:ActorRef)
  }

  object Tasks {
    case class AnnouncePeers()
    case class ReconnectPeers()
  }
}

class P2p(ourGuid:String, seeds:Seq[String]) extends Actor {
  private val log =  Logging(context.system, this)
  log.info("p2p starting up with seeds {}", seeds)

  private var peers = Map.empty[String, ActorRef]
  private var knownPaths = seeds.map(hostportToActorPath).toSet
  private var subscribers = Set.empty[ActorRef]
  import context.dispatcher

  context.system.scheduler.schedule(1 seconds, 1 seconds,
    context.self, P2p.Tasks.AnnouncePeers())
  context.system.scheduler.schedule(2 second, 2 second,
    context.self, P2p.Tasks.ReconnectPeers())

  def hostportToActorPath(s:String) = ActorPath.fromString(s"akka.tcp://Main@$s/user/app/networkSync/p2p")

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
    case P2p.NetMessages.AnnouncePeers(newPaths) =>
      log.info("got new paths {}", newPaths.diff(knownPaths))
      knownPaths = knownPaths ++ newPaths
    case P2p.Messages.Subscribe(subscriber) => subscribers = subscribers + subscriber
    case P2p.Messages.Broadcast(payload) => peers.foreach(_._2 ! P2p.NetMessages.Message(ourGuid, payload)) // wtf?
    case P2p.Messages.Send(guid, payload) => peers.get(guid).foreach(_ ! P2p.NetMessages.Message(ourGuid, payload))
    case P2p.Tasks.AnnouncePeers() => peers.foreach(_._2 ! P2p.NetMessages.AnnouncePeers(knownPaths))
    case P2p.Tasks.ReconnectPeers() =>
      val me = context.self
      val connectedPaths = peers.values.map(_.path).toSet + hostportToActorPath(
        context.system.settings.config.getString("akka.remote.netty.tcp.hostname")+ ":" +
          context.system.settings.config.getInt("akka.remote.netty.tcp.port").toString)
      val toReconnect = knownPaths.diff(connectedPaths)
      log.info(s"reconnecting peers $toReconnect")

      toReconnect.foreach(path =>
        context.actorSelection(path).resolveOne(5 seconds).andThen({
          case Success(ref:ActorRef) =>
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
    case Terminated(ref) => peers.find(_._2 == ref).map(p => peers = peers - p._1)
  }
}