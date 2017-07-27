import java.util.UUID

import org.scalatest.{Matchers, WordSpecLike}
import akka.actor.{Actor, ActorContext, ActorRef, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestActors, TestKit}
import com.roundeights.hasher.Hash
import moshpit.{VClock, actors}
import moshpit.actors._
import org.joda.time.DateTime
import org.scalatest.concurrent.{Eventually, IntegrationPatience, PatienceConfiguration, ScalaFutures}
import org.scalatest.{BeforeAndAfterAll, Inside, Matchers, WordSpecLike}
import org.scalatest.Matchers._
import com.roundeights.hasher.Hash
import com.roundeights.hasher.Implicits._
import org.scalacheck.Gen
import org.scalacheck.Gen.Choose

import scala.concurrent.duration._
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.time.{Milliseconds, Seconds, Span}
import cats._
import cats.implicits._
import cats.data._
import org.scalacheck.Test.Parameters

import scala.concurrent.Future

object P2pMock {
  object Messages {
    case class ExtraSubscriber(guid:String, ref:ActorRef)
  }
}

class P2pMock(ourGuid:String) extends Actor {
  var subscribers = Map.empty[String, ActorRef]

  override def receive: Receive = {
    case P2p.Messages.Send(guid, payload) =>
//      Console.println(s"sending towards $guid $payload")
      //val sendingSubscriber = subscribers.find(_._2 == sender()).get
      subscribers(guid) ! P2p.NetMessages.Message(ourGuid, payload)
    case P2p.Messages.Broadcast(payload) =>
      //Console.println(s"broadcasting towards everyone")
//      Console.println(s"$ourGuid mock broadcast $payload")
      val sendingSubscriber = subscribers.find(_._2 == sender()).get
      subscribers.filter(_._1 != ourGuid).values.foreach(_ ! P2p.NetMessages.Message(sendingSubscriber._1, payload))
    case P2p.Messages.Subscribe(subscriber) =>
      self ! P2pMock.Messages.ExtraSubscriber(ourGuid, subscriber)
    case P2pMock.Messages.ExtraSubscriber(sguid, ref) =>
      subscribers = subscribers.updated(sguid, ref)
    case msg@P2p.NetMessages.Message(snd, payload) =>
//      Console.println(s"receiving from $snd $payload towards $ourGuid")
      subscribers(ourGuid) ! msg
  }
}

class MockP2pFactory(p2p:ActorRef) extends P2pFactory {
  override def spawnP2p(context: ActorContext, ourGuid: String, appDbRef: ActorRef, seeds: Seq[String], actorName:String): ActorRef = {
    p2p
  }
}

class NetworkSyncSpec extends TestKit(ActorSystem("networkSyncTest"))
  with WordSpecLike with Matchers with BeforeAndAfterAll with ScalaFutures with Inside with GeneratorDrivenPropertyChecks
  with IntegrationPatience with Eventually with ImplicitSender {
  override def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  val testerGuid = "tester"
  val maxApps = 6
  val maxInstancesPerApp = 6

  def spawnNs() = {
    val guid = UUID.randomUUID().toString
    Console.println(s"spawning ns with $guid")
    val p2p = system.actorOf(Props(new P2pMock(guid)))
    val appDb = system.actorOf(AppDb.props(guid, 60, 60, 3200))
    val ns = system.actorOf(NetworkSync.props(guid, Seq.empty, appDb, new MockP2pFactory(p2p)))
    (guid, p2p, appDb, ns)
  }


  "NetworkSync" must {
    "periodically advertise root hash" in {
      val (guid, p2p, appDb, ns) = spawnNs()
      p2p ! P2pMock.Messages.ExtraSubscriber(testerGuid, self)
      val appDbProxy = new AppDbProxy(appDb)
      val hash = { whenReady(appDbProxy.queryRootHash()) { hash => hash } }
      expectMsg(P2p.NetMessages.Message(guid, NetworkSync.Messages.AdvertiseRootHash(hash)))
    }

    "asks for apps when hash mistmatch" in {
      ignoreMsg({case (P2p.NetMessages.Message(_, NetworkSync.Messages.AdvertiseRootHash(_))) => true })
      val (guid, p2p, appDb, ns) = spawnNs()
      p2p ! P2pMock.Messages.ExtraSubscriber(testerGuid, self)
      val appDbProxy = new AppDbProxy(appDb)
      val reqHash = "wrong".md5.hash
      p2p ! P2p.NetMessages.Message(testerGuid, NetworkSync.Messages.AdvertiseRootHash(reqHash))
      expectMsg(P2p.NetMessages.Message(guid, NetworkSync.Messages.RequestApps(reqHash)))
    }

    "pushing all apps to us on request" in {
      forAll (Gen.choose[Int](1, maxApps), Gen.choose[Int](1, maxInstancesPerApp)){ (nApps: Int, nInstances:Int) =>
        ignoreMsg({ case (P2p.NetMessages.Message(_, NetworkSync.Messages.AdvertiseRootHash(_))) => true })
        val (guid, p2p, appDb, ns) = spawnNs()
        p2p ! P2pMock.Messages.ExtraSubscriber(testerGuid, self)
        val appDbProxy = new AppDbProxy(appDb)
        val apps = (0 until nApps).map(_.toString)
        val instances = (0 until nInstances).map(id => UUID.randomUUID().toString)
        for {appId <- apps
             instanceId <- instances} {
          appDbProxy.updateInstance(appId, instanceId, s"data of $appId::$instanceId")
        }

        val hash = { whenReady(appDbProxy.queryRootHash()) { hash => hash } }
        p2p ! P2p.NetMessages.Message(testerGuid, NetworkSync.Messages.RequestApps(hash))
        val appsHashes = { whenReady(appDbProxy.queryApps()) { result => result } }
        expectMsg(P2p.NetMessages.Message(guid, NetworkSync.Messages.PushApps(appsHashes)))
      }
    }

    object DbActions {
      trait DbAction {
        def run(appDbProxy: AppDbProxy): Future[Any]
      }

      case class UpdateInstance(appId: String, instanceId: String, data: String) extends DbAction {
        import system.dispatcher
        override def run(appDbProxy: AppDbProxy): Future[Any] = Future {
          appDbProxy.updateInstance(appId, instanceId, data)
        }.mapTo[Any]
      }

      lazy val shortRandomId = Gen.listOfN(1, Gen.alphaChar).map(_.mkString)

      lazy val genUpdateAction = for {
        appId <- shortRandomId
        instanceId <- shortRandomId
        data <- Gen.alphaStr
      } yield UpdateInstance(appId, instanceId, data)

      case class PingInstance(appId: String, instanceId: String) extends DbAction {
        override def run(appDbProxy: AppDbProxy): Future[Any] = appDbProxy.pingInstance(appId, instanceId).mapTo[Any]
      }

      lazy val genPingInstance = for {
        appId <- shortRandomId
        instanceId <- shortRandomId
      } yield PingInstance(appId, instanceId)

      case class DeleteInstance(appId: String, instanceId: String) extends DbAction {
        override def run(appDbProxy: AppDbProxy): Future[Any] = appDbProxy.deleteInstance(appId, instanceId).mapTo[Any]
      }

      lazy val genDeleteInstance = for {
        appId <- shortRandomId
        instanceId <- shortRandomId
      } yield DeleteInstance(appId, instanceId)


      lazy val gens = Seq(genUpdateAction, genUpdateAction, genPingInstance, genDeleteInstance)
      lazy val gen:Gen[DbAction] = for { g <- Gen.oneOf(gens)
                                          r <- g } yield r
    }

    case class MultiSyncTestCase(nDbs:Int, actions:List[DbActions.DbAction], onDbs:List[Set[Int]])
    lazy val genMultiSyncTestCase = for {
      nDbs <- Gen.choose[Int](3, 5)
      nActions <- Gen.choose[Int](1, 64)
      actions <- Gen.listOfN(nActions, DbActions.gen)
      onDbs <- Gen.listOfN(actions.size, Gen.nonEmptyListOf[Int](Gen.choose[Int](0, nDbs - 1)).map(_.toSet))
    } yield MultiSyncTestCase(nDbs, actions, onDbs)


    "syncs N databases to the state of independent database" in {
      forAll(genMultiSyncTestCase) { case MultiSyncTestCase(nDbs, actions, onDbs) =>
        val nss = (1 to nDbs).map(_ => spawnNs()).toSet
        // susscribe to each others p2p
        for {ns <- nss} {
          for {rs <- nss - ns} {
            val (nguid, np2p, _, _) = ns
            val (rguid, rp2p, _, _) = rs
            rp2p ! P2pMock.Messages.ExtraSubscriber(nguid, np2p)
          }
        }

        // conduct actions
        val dbProxies = nss.map({ case (guid, p2p, appDb, ns) => new AppDbProxy(appDb) })
        val awaiters = for {(action, onDb) <- actions.zip(onDbs)} yield {
          val selectedDbProxies = dbProxies.zipWithIndex.filter(p => onDb.contains(p._2)).map(_._1)
          selectedDbProxies.map(p => action.run(p))
        }

        for {awaiter <- awaiters.flatten} {
          whenReady(awaiter) { result => }
        }

        eventually {
          val hashes = dbProxies.map(proxy => whenReady(proxy.queryRootHash()) { hash => hash })
          val referenceHash = hashes.head
          hashes.toSet shouldEqual Set(referenceHash)
        }
        Console.println("!!!! success !!!!!")
      }
    }
  }
}
