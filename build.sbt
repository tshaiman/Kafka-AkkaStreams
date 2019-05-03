name := "kafka-streamser"

version := "0.1"

scalaVersion := "2.12.8"

lazy val akkaVersion = "2.5.22"
lazy val playVersion = "2.7.3"
lazy val akkaStreamKafka = "1.0.1"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-cluster" % akkaVersion,
  "com.typesafe.akka" %% "akka-distributed-data" % akkaVersion,
  "com.typesafe.play" %% "play-json" % playVersion,
  "com.typesafe.akka" %% "akka-stream-kafka" % akkaStreamKafka,

  "com.typesafe.akka" %% "akka-testkit" % akkaVersion,
  "com.typesafe.akka" %% "akka-multi-node-testkit" % akkaVersion,
  "org.scalatest" %% "scalatest" % "3.0.7" % "test"
)