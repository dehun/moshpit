import java.util.UUID

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestActors, TestKit}
import moshpit.{VClock, actors}
import moshpit.actors.{AppDb, AppDbProxy, InstanceMetaInfo}
import org.scalatest.concurrent.{IntegrationPatience, PatienceConfiguration, ScalaFutures}
import org.scalatest.{BeforeAndAfterAll, Inside, Matchers, WordSpecLike}
import org.scalatest.Matchers._

import scala.concurrent.duration._
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.time.{Milliseconds, Seconds, Span}


class AppDbSpec() extends TestKit(ActorSystem("appDbTest")) with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterAll with ScalaFutures with Inside with GeneratorDrivenPropertyChecks
  with IntegrationPatience {

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

    val maxApps = 4
    val maxInstancesPerApp = 4

    "query apps" in {
      forAll { (k: Int) =>
        val nApps = k.abs % maxApps
        val nInstances = 1 + k.abs % maxInstancesPerApp // we want at least one instance for this test
      val apps = (0 until nApps).map(_.toString)
        val instances = (0 until nInstances).map(id => UUID.randomUUID().toString)
        val appDbProxy = new AppDbProxy(system.actorOf(AppDb.props("so_random_guid", 60, 60, 1200)))
        for {appId <- apps
             instanceId <- instances} {
          appDbProxy.updateInstance(appId, instanceId, s"data of $appId::$instanceId")
        }
        whenReady(appDbProxy.queryApps(), PatienceConfiguration.Timeout(Span(10, Seconds))) { result =>
          result.keySet should ===(apps.toSet)
        }
      }
    }

    "register with the same instance id but different appId works in isolation" in {
      val appDbProxy = new AppDbProxy(system.actorOf(AppDb.props("so_random_guid", 60, 60, 1200)))
      appDbProxy.updateInstance("appid_one", "clashing_instance", s"wow")
      appDbProxy.updateInstance("appid_two", "clashing_instance", s"wow")

      whenReady(appDbProxy.queryApp("appid_one", stripped = true)) { result =>
        result.keySet should ===(Set("clashing_instance"))
      }

      whenReady(appDbProxy.queryApp("appid_two", stripped = true)) { result =>
        result.keySet should ===(Set("clashing_instance"))
      }

      whenReady(appDbProxy.deleteInstance("appid_one", "clashing_instance")) { result =>
        result should ===(AppDb.Messages.DeleteInstance.Success())
      }

      whenReady(appDbProxy.queryApp("appid_two", stripped = true)) { result =>
        result.keySet should ===(Set("clashing_instance"))
      }

      whenReady(appDbProxy.deleteInstance("appid_two", "clashing_instance")) { result =>
        result should ===(AppDb.Messages.DeleteInstance.Success())
      }

    }

    "register and delete all" in {
      forAll { (k: Int) =>
        val nApps = k.abs % maxApps
        val nInstances = k.abs % maxInstancesPerApp
        val apps = (0 until nApps).map(_.toString)
        val instances = (0 until nInstances).map(id => UUID.randomUUID().toString)
        var registered = Set.empty[(String, String)]
        val appDbProxy = new AppDbProxy(system.actorOf(AppDb.props("so_random_guid", 60, 60, 1200)))
        for {appId <- apps
             instanceId <- instances} {
          appDbProxy.updateInstance(appId, instanceId, s"data of $appId::$instanceId")
          registered += ((appId, instanceId))
        }

        var deleted = Set.empty[(String, String)]
        for {r <- registered} {
          val (appId, instanceId) = r
          whenReady(appDbProxy.deleteInstance(appId, instanceId)) { result =>
            result should ===(AppDb.Messages.DeleteInstance.Success())
          }
          deleted += r
          val thisAppRegisteredInstances = registered.diff(deleted).filter(_._1 == appId).map(_._2)
          whenReady(appDbProxy.queryApp(appId, stripped = true)) { result =>
            result.keySet should ===(thisAppRegisteredInstances)
          }
        }

        // apps are still present, however all of them are empty
        whenReady(appDbProxy.queryApps()) { result =>
          result.keySet should ===(apps.toSet)
        }

        for (appId <- apps) {
          whenReady(appDbProxy.queryApp(appId, stripped = true)) { result =>
            result should ===(Map.empty)
          }
        }
      }
    }
  }
}
