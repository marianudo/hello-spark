import sbt._

object Dependencies {
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.8"
  lazy val sparkCore = "org.apache.spark" %% "spark-core" % "2.4.5" % "provided"
}
