package moshpit

import com.roundeights.hasher.Hash


object VClock {
  import moshpit.Hashable._
  def resolve(resolverId:String, lhs: VClock, rhs: VClock):VClock =
    if (lhs.isSubclockOf(rhs)) rhs
    else if (rhs.isSubclockOf(lhs)) lhs
    else {
      val newStamps = lhs.stamps.keySet.union(rhs.stamps.keySet).map(k =>
        (k, List(lhs.stamps.get(k), rhs.stamps.get(k)).filter(_.isDefined).map(_.get).max)).toMap
      new VClock(newStamps).update(resolverId)
    }
  def empty = new VClock(Map.empty[String, Int])

  implicit def vclock2Hashable(vc:VClock):Hashable = vc.stamps
}

case class VClock(stamps: Map[String, Int]) {
  def isSubclockOf(of:VClock):Boolean =
    stamps.keySet.subsetOf(of.stamps.keySet) &&
      stamps.keySet.forall(k => stamps(k) <= of.stamps(k))

  def isConflicting(other:VClock):Boolean = !(other.isSubclockOf(this) || this.isSubclockOf(other))

  def update(requester:String):VClock = {
    val newStamp = stamps.get(requester).map(_ + 1).getOrElse(1)
    new VClock(stamps.updated(requester, newStamp))
  }

  override def toString: String = s"VClock{$stamps}"
}
