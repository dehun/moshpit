import moshpit.VClock
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{Matchers, WordSpecLike}
import org.scalacheck.Gen


class VClockSpec extends  Matchers with WordSpecLike  with GeneratorDrivenPropertyChecks {
  lazy val genClock:Gen[VClock] = for {
    n <- Gen.choose(0, 100)
    vs <- Gen.listOfN(n, Gen.choose(0, 100))
  } yield VClock((0 until n).map(i => (i.toString, vs(i))).toMap)

  "VClock" must {
    "later clock is not subclock" in {
      forAll(genClock) { (vc:VClock) =>
        vc.update("1").isSubclockOf(vc) shouldBe false

      }
    }

    "be subblockOf later clock" in {
      forAll(genClock) { (vc:VClock) =>
        vc.isSubclockOf(vc.update("1")) shouldBe true
      }
    }
  }
}
