package moshpit

object VClock {
  def resolve(lhs: VClock, rhs: VClock):VClock =
    if (lhs.isSubclockOf(rhs)) rhs
    else if (rhs.isSubclockOf(lhs)) lhs
    else {
      val newStamps = lhs.stamps.keySet.union(rhs.stamps.keySet).map(k =>
        (k, List(lhs.stamps.get(k), rhs.stamps.get(k)).filter(_.isDefined).map(_.get).max)).toMap
      new VClock(newStamps)
    }
  def empty = new VClock(Map.empty[String, Int])
}

class VClock(val stamps: Map[String, Int]) {
  def isSubclockOf(of:VClock):Boolean =
    stamps.keySet.subsetOf(of.stamps.keySet) &&
      stamps.keySet.forall(k => stamps(k) <= of.stamps(k))

  def update(requester:String):VClock = {
    val newStamp = stamps.get(requester).map(_ + 1).getOrElse(1)
    new VClock(stamps.updated(requester, newStamp))
  }

  override def toString: String = s"VClock{$stamps}"
}
