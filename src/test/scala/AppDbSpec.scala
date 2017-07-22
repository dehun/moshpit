import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestActors, TestKit}
import moshpit.{VClock, actors}
import moshpit.actors.AppDbProxy
import moshpit.actors.AppDb
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import org.scalatest.Matchers._


class AppDbSpec() extends TestKit(ActorSystem("appDbTest")) with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterAll with ScalaFutures {

  override def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  "AppDb" must {
    "update new instance" in {
      val appDbProxy = new AppDbProxy(system.actorOf(AppDb.props("so_random_guid", 60, 60, 1200)))
      appDbProxy.updateInstance("so_app_id", "so_instance_guid", "wow")
      whenReady(appDbProxy.queryInstance("so_app_id", "so_instance_guid")) {
        case AppDb.Messages.QueryInstance.Success(meta, data) =>
          meta.appId shouldEqual "so_app_id"
          meta.wasDeleted shouldEqual false
          meta.instanceTtlSec shouldEqual 60
          meta.vclock shouldEqual VClock(Map("so_random_guid" -> 1))
      }
    }
  }
}
