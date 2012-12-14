import sbt._
import Keys._


object CollectionsBuild extends Build {
  val scalatest = "org.scalatest" %% "scalatest" % "2.0.M5" % "test"


  lazy val defaultSettings = Defaults.defaultSettings ++ Seq(
      libraryDependencies += scalatest)
    
  

  lazy val root = Project(id = "collections",
                          base = file("."),
                          settings = defaultSettings)
}

