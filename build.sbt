name := "moshpit"
version := "1.0"
scalaVersion := "2.12.2"

fork := true
//retrieveManaged := true
javaOptions := Seq("-Dmx=1024M")

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"
resolvers += Resolver.url("bintray-sbt-plugins", url("http://dl.bintray.com/sbt/sbt-plugin-releases"))(Resolver.ivyStylePatterns)
//addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.4")

libraryDependencies ++=
  Seq("com.typesafe.akka" %% "akka-actor" % "2.5.3",
       "com.typesafe.akka" %% "akka-remote" % "2.5.3",
       "org.typelevel" %% "cats" % "0.9.0",
       "com.roundeights" %% "hasher" % "1.2.0",
       "com.typesafe.akka" %% "akka-http" % "10.0.9",
       "com.typesafe.akka" %% "akka-http-spray-json" % "10.0.7",
       "com.github.nscala-time" %% "nscala-time" % "2.16.0")

mainClass in Compile := Some("moshpit.Main")