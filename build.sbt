import spray.boilerplate.BoilerplatePlugin

import Common._
import Dependencies._

lazy val root = (project in file("."))
  .settings(commonSettings)
  .settings(
    name := "root"
  )
  .dependsOn(akua)
  .aggregate(akua)

lazy val akua = (project in file("./akua"))
  .enablePlugins(BoilerplatePlugin)
  .settings(commonSettings)
  .settings(
    name := "akua",

    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-stream" % akkaVersion,
      "com.chuusai" %% "shapeless" % shapelessVersion,
      "org.mapdb" % "mapdb" % mapDbVersion
    ),

    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion
    ).map(_ % "test"),

    fork in Test := true
  )
