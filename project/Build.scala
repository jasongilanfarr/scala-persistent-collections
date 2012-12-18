import sbt._
import Keys._
import com.typesafe.sbteclipse.plugin.EclipsePlugin.EclipseKeys

object CollectionsBuild extends Build {
  val scalatest = "org.scalatest" %% "scalatest" % "2.0.M5" % "test"
  EclipseKeys.eclipseOutput := Some(".target")

  lazy val defaultSettings = Defaults.defaultSettings ++ Seq(
      libraryDependencies += scalatest)
    
  

  lazy val root = Project(id = "collections",
                          base = file("."),
                          settings = defaultSettings)
}

