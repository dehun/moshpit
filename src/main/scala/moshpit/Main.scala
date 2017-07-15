package moshpit

object Main {
  def main(args: Array[String]): Unit = {
    akka.Main.main(Array("moshpit.actors.MoshpitMain"))
  }
}
