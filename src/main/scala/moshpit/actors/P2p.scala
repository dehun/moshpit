package moshpit.actors

import akka.actor.{Actor, Props}
import akka.event.Logging

object P2p {
  def props(guid:String, seeds:Seq[String]) = Props(new P2p(guid, seeds))
}

class P2p(guid:String, seeds:Seq[String]) extends Actor {
  private val log =  Logging(context.system, this)
  log.info("p2p starting up with seeds {}", seeds)
  override def receive: Receive = ???
}
