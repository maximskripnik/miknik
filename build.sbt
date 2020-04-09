ThisBuild / scalaVersion := "2.13.1"
ThisBuild / version := "0.0.1"
ThisBuild / organization := "com.newflayer"

lazy val root = (project in file("."))
  .settings(
    name := "miknik",
    libraryDependencies ++= Dependencies.Compile.all ++ Dependencies.Test.all,
    scalafixDependencies ++= Dependencies.Scalafix.all,
    scalacOptions ++= List(
      "-Wunused"
    ),
    coverageExcludedPackages := "com.newflayer.miknik.bootstrap"
  )

ThisBuild / scalafixDependencies ++= Dependencies.Scalafix.all
addCommandAlias(
  "scalafixCheckAll",
  "; compile:scalafix --check ; test:scalafix --check"
)
