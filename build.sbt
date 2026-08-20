ThisBuild / scalaVersion := "2.12.18"
ThisBuild / version := "0.1.0"

lazy val root = (project in file("."))
  .settings(
    name := "spark-module-tracer-demo",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.5.1"
    )
  )
