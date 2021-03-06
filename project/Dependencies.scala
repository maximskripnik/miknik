import sbt._

object Dependencies {
  object Compile {
    val akka = "com.typesafe.akka" %% "akka-actor-typed" % "2.6.4"
    val akkaHttp = "com.typesafe.akka" %% "akka-http" % "10.1.11"
    val akkaStreams = "com.typesafe.akka" %% "akka-stream" % "2.6.4"
    val logback = "ch.qos.logback" % "logback-classic" % "1.2.3"
    val cats = "org.typelevel" %% "cats-core" % "2.1.0"
    val circe = "io.circe" %% "circe-core" % "0.12.3"
    val circeGeneric = "io.circe" %% "circe-generic" % "0.12.3"
    val akkaHttpCirce = "de.heikoseeberger" %% "akka-http-circe" % "1.31.0"
    val chimney = "io.scalaland" %% "chimney" % "0.5.0"
    val mesos = "org.apache.mesos" % "mesos" % "1.9.0"
    val alpakkaSimpleCodecs = "com.lightbend.akka" %% "akka-stream-alpakka-simple-codecs" % "1.1.2"
    val digitaloceanApi = "com.myjeeva.digitalocean" % "digitalocean-api-client" % "2.17"
    val scalaSsh = "com.decodified" %% "scala-ssh" % "0.10.0"
    val sqliteJdbc = "org.xerial" % "sqlite-jdbc" % "3.31.1"

    val all = List(
      akka,
      akkaHttp,
      akkaStreams,
      logback,
      cats,
      circe,
      circeGeneric,
      akkaHttpCirce,
      chimney,
      mesos,
      alpakkaSimpleCodecs,
      digitaloceanApi,
      scalaSsh,
      sqliteJdbc
    )
  }

  object Test {
    val scalaTest = "org.scalatest" %% "scalatest" % "3.1.1"
    val mockito = "org.mockito" %% "mockito-scala" % "1.13.5"
    val mockitocats = "org.mockito" %% "mockito-scala-cats" % "1.13.5"
    val scalaCheck = "org.scalacheck" %% "scalacheck" % "1.14.1"
    val scalaTestPlusScalaCheck = "org.scalatestplus" %% "scalacheck-1-14" % "3.1.0.0"
    val akkaActorTestKit = "com.typesafe.akka" %% "akka-actor-testkit-typed" % "2.6.4"
    val akkaHttpTestKit = "com.typesafe.akka" %% "akka-http-testkit" % "10.1.11"
    val akkaStreamTestKit = "com.typesafe.akka" %% "akka-stream-testkit" % "2.6.4"

    val all = List(
      scalaTest,
      mockito,
      mockitocats,
      scalaCheck,
      scalaTestPlusScalaCheck,
      akkaActorTestKit,
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
