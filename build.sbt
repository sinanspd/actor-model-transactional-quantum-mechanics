ThisBuild / scalaVersion := "3.3.7"

val PekkoVersion = "1.4.0"

libraryDependencies ++= Seq(
  "org.apache.pekko" %% "pekko-actor-typed" % PekkoVersion,
  "ch.qos.logback" % "logback-classic" % "1.5.24"
)
