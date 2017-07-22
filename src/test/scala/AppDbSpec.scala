import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestActors, TestKit}
import moshpit.{VClock, actors}
import moshpit.actors.{AppDb, AppDbProxy, InstanceMetaInfo}
import org.scalatest.concurrent.{PatienceConfiguration, ScalaFutures}
import org.scalatest.{BeforeAndAfterAll, Inside, Matchers, WordSpecLike}
import org.scalatest.Matchers._

import scala.concurrent.duration._
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.time.{Seconds, Span}


class AppDbSpec() extends TestKit(ActorSystem("appDbTest")) with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterAll with ScalaFutures with Inside with GeneratorDrivenPropertyChecks {

  override def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  "AppDb" must {
    "update new instance" in {
      val appDbProxy = new AppDbProxy(system.actorOf(AppDb.props("so_random_guid", 60, 60, 1200)))
      appDbProxy.updateInstance("so_app_id", "so_instance_guid", "wow")
      whenReady(appDbProxy.queryInstance("so_app_id", "so_instance_guid")) {
        case AppDb.Messages.QueryInstance.Success(meta, data) =>
          inside(meta) { case InstanceMetaInfo(vclock, _, wasDeleted, instanceTtlSec, appId) =>
            appId shouldEqual "so_app_id"
            wasDeleted shouldEqual false
            instanceTtlSec shouldEqual 60
            vclock shouldEqual VClock(Map("so_random_guid" -> 1))
          }
      }
    }

    "ping existing instance" in {
      val appDbProxy = new AppDbProxy(system.actorOf(AppDb.props("so_random_guid", 60, 60, 1200)))
      appDbProxy.updateInstance("so_app_id", "so_instance_guid", "wow")
      whenReady(appDbProxy.pingInstance("so_app_id", "so_instance_guid")) {
        case AppDb.Messages.PingInstance.Success() =>
      }
    }

    "ping non existing instance" in {
      val appDbProxy = new AppDbProxy(system.actorOf(AppDb.props("so_random_guid", 60, 60, 1200)))
      appDbProxy.updateInstance("so_app_id", "so_instance_guid", "wow")
      whenReady(appDbProxy.pingInstance("so_app_id", "not_so_instance_guid")) {
        case AppDb.Messages.PingInstance.NotExists() =>
      }
    }

    "query apps" in {
      forAll { (k:Int) =>
        val n = k.abs % 1000
        val appDbProxy = new AppDbProxy(system.actorOf(AppDb.props("so_random_guid", 60, 60, 1200)))
        for (id <- 0 until n) {
          appDbProxy.updateInstance(id.toString, id.toString, s"data of $id")
        }
        whenReady(appDbProxy.queryApps(), PatienceConfiguration.Timeout(Span(10, Seconds))) { result =>
          result.keySet should === (List.range(0, n).map(_.toString).toSet)
        }
      }
    }
  }
}
