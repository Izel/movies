name := "movies"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.5"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.5"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.1.1" % "test"

resolvers += "Artima Maven Repository" at "http://repo.artima.com/releases"