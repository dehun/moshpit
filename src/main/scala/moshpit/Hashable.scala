package moshpit

import com.roundeights.hasher.Hash
import com.roundeights.hasher.Implicits._
import scala.language.implicitConversions
import com.github.nscala_time.time.Imports.DateTime

trait Hashable {
  def hash:Hash
}

object Hashable {
  implicit def bool2Hashable(x:Boolean):Hashable = new Hashable {
    override def hash: Hash = x.toString.hash
  }

  implicit def int2Hashable(x:Int):Hashable = new Hashable {
    override def hash: Hash = x.toString.hash
  }

  implicit def datetime2Hashable(x:DateTime):Hashable = new Hashable {
    override def hash: Hash = x.toString.hash
  }

  implicit def string2Hashable(x:String):Hashable = new Hashable {
    override def hash: Hash = x.sha256
  }

  implicit def tuple2Hashable[T1, T2](x:(T1, T2))(implicit toHashable1: T1 => Hashable, toHashable2: T2 => Hashable):Hashable = new Hashable {
    override def hash: Hash = (x._1.hash.hex ++ x._2.hash.hex).hash
  }

  implicit def list2Hashable[T](xs:List[T])(implicit toHashable: T => Hashable):Hashable = new Hashable {
    override def hash: Hash = xs.map(_.hash).toString().hash
  }

  implicit def map2Hashable[K,V](xs:Map[K, V])(implicit toHashableK: K => Hashable, toHashableV: V => Hashable, cmp:Ordering[K]):Hashable = new Hashable {
    override def hash: Hash = xs.toList.sortBy(_._1).hash
  }

  implicit def set2Hashable[T](xs:Set[T])(implicit toHashable:T => Hashable):Hashable = new Hashable {
    override def hash: Hash = xs.toList.map(_.hash.hex).sorted.hash
  }
}
