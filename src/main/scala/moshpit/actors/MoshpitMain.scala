package moshpit.actors

import akka.actor._
import akka.event.Logging

import scala.collection.JavaConverters._

class MoshpitMain extends Actor {
  private val peerGuid:String = java.util.UUID.randomUUID().toString
  private val log =  Logging(context.system, this)
  log.info("starting with guid", peerGuid)

  val seeds = context.system.settings.config.getStringList("moshpit.seeds").asScala
  private val p2p = context.actorOf(P2p.props(peerGuid, seeds), "p2p")
  private val appDb = context.actorOf(Props[AppDb], "appDb")
  private val networkSync = context.actorOf(NetworkSync.props(p2p, appDb), "networkSync")

  override def receive: Receive = {
    case x => Console.println(x)
  }
}
