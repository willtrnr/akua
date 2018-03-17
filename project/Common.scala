import sbt._
import sbt.Keys._

import Dependencies._

object Common {

  val commonSettings = Seq(
    organization := "net.archwill.akua",

    scalaVersion := "2.12.4",
    crossScalaVersions := Seq("2.11.11", "2.12.4"),

    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % scalaTestVersion
    ).map(_ % "test"),

    libraryDependencies ++= Seq(
      compilerPlugin("org.psywerx.hairyfotr" %% "linter" % linterVersion)
    ),

    javacOptions ++= Seq(
      "-deprecation",
      "-encoding", "UTF-8",
      "-source", "1.8",
      "-target", "1.8",
      "-Xlint"
    ),

    scalacOptions ++= Seq(
      "-deprecation",
      "-encoding", "UTF-8",
      "-feature",
      "-language:existentials",
      "-language:higherKinds",
      "-language:implicitConversions",
      "-target:jvm-1.8",
      "-unchecked",
      "-Xfatal-warnings",
      "-Xfuture",
      "-Xlint",
      "-Yno-adapted-args",
      "-Ypartial-unification",
      "-Ywarn-dead-code",
      "-Ywarn-numeric-widen",
      "-Ywarn-unused-import"
    ),

    scalacOptions in (Compile, console) --= Seq("-Ywarn-unused:imports", "-Xfatal-warnings"),
    scalacOptions in (Test, console) --= Seq("-Ywarn-unused:imports", "-Xfatal-warnings"),

    testOptions += Tests.Argument("-oF"),

    fork in Test := true,

    autoAPIMappings := true
  )

}
