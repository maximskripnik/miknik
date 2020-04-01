import sbt._

object Dependencies {
  object Compile {
    val akka = "com.typesafe.akka" %% "akka-actor-typed" % "2.6.4"
    val akkaHttp = "com.typesafe.akka" %% "akka-http" % "10.1.11"
    val akkaStreams = "com.typesafe.akka" %% "akka-stream" % "2.6.4"
    val logback = "ch.qos.logback" % "logback-classic" % "1.2.3"

    val all = List(
      akka,
      akkaHttp,
      akkaStreams,
      logback
    )
  }

  object Test {
    val scalaTest = "org.scalatest" %% "scalatest" % "3.1.1"
    val akkaHttpTestKit = "com.typesafe.akka" %% "akka-http-testkit" % "10.1.11"
    val akkaStreamTestKit = "com.typesafe.akka" %% "akka-stream-testkit" % "2.6.4"

    val all = List(
      scalaTest,
      akkaHttpTestKit,
      akkaStreamTestKit
    ).map(_ % Configurations.Test)
  }

  object Scalafix {
    val sortImports = "com.nequissimus" %% "sort-imports" % "0.3.2"

    val all = List(
      sortImports
    )
  }
}
