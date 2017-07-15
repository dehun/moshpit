name := "moshpit"
version := "1.0"
scalaVersion := "2.12.0"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies ++=
  Seq("com.typesafe.akka" %% "akka-actor" % "2.5.3",
       "com.typesafe.akka" %% "akka-remote" % "2.5.3")
