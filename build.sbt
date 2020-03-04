resolvers += "Artima Maven Repository" at "http://repo.artima.com/releases"

lazy val root = (project in file("."))
  .settings(
    name := "movies",
    version := "0.1",
    scalaVersion := "2.11.8",
    libraryDependencies ++= List(
      "org.apache.spark" %% "spark-core" % "2.4.5",
      "org.apache.spark" %% "spark-sql" % "2.4.5",
      "org.scalatest" %% "scalatest" % "3.1.1" % "test"
    )
  )