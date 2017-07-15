package moshpit.actors

import akka.actor.{Actor, ActorRef, Props}

object NetworkSync {
  def props(p2pRef:ActorRef, appDbRef:ActorRef) = Props(new NetworkSync(p2pRef, appDbRef))
}

class NetworkSync(p2pRef:ActorRef, appDbRef:ActorRef) extends Actor {
  override def receive: Receive = ???
}
