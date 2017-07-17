package moshpit.actors

import akka.actor._
import akka.event.Logging
import com.typesafe.config.ConfigFactory

import scala.collection.JavaConverters._

class MoshpitMain extends Actor {
  private val peerGuid:String = java.util.UUID.randomUUID().toString
  private val log =  Logging(context.system, this)
  log.info(s"starting with $peerGuid")

  private val config = ConfigFactory.load()
  private val seeds = config.getStringList("moshpit.seeds").asScala
  private val appDb = context.actorOf(AppDb.props(peerGuid), "appDb")
  private val networkSync = context.actorOf(NetworkSync.props(peerGuid, seeds, appDb), "networkSync")

  override def receive: Receive = {
    case x => Console.println(x)
  }
}
