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

    "updating works" in {
      forAll(genClock, Gen.choose(0, 100)) { (vc:VClock, k:Int) =>
        val requester = (if (vc.stamps.nonEmpty) k % vc.stamps.size else 1).toString
        val updated = vc.update(requester)
        updated.stamps.get(requester).exists(v => v > vc.stamps.getOrElse(requester, 0)) shouldBe true
      }
    }

    "clock is subclock of self" in {
      forAll(genClock) { (vc:VClock) =>
        vc.isSubclockOf(vc) shouldBe true
      }
    }

    "empty is subclock of everything" in {
        forAll(genClock) { (vc:VClock) =>
          VClock.empty.isSubclockOf(vc) shouldBe true
      }
    }

    "resolution works" in {
      forAll (Gen.nonEmptyListOf(genClock)) { (clocks) =>
        val rvc = clocks.fold(VClock.empty) { (acc, vc) => VClock.resolve("somebody", acc, vc) }
        clocks.foreach({vc => vc.isSubclockOf(rvc) shouldBe true})
      }
    }

    "hashing works" in {
      forAll (genClock) { (vc) =>
        vc.update("1").update("2").hash should === (vc.update("2").update("1").hash)
      }
    }
  }
}
