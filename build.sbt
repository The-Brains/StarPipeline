lazy val core = RootProject(file("../CommonLib"))

name := "StarPipeline"

version := "0.1"

organization := "com.thebrains"

scalaVersion := "2.11.12"

dependsOn(core)

libraryDependencies += "org.rogach" %% "scallop" % "3.1.2"

libraryDependencies += "com.sksamuel.elastic4s" %% "elastic4s-core" % "6.2.3"