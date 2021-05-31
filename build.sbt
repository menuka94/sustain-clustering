import sbt.Keys.libraryDependencies

name := "sustain-clustering"

version := "0.0.4"

scalaVersion := "2.12.10"

resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

libraryDependencies ++= Seq(
  "org.mongodb.spark" %% "mongo-spark-connector" % "3.0.1",
  "org.apache.spark" %% "spark-core" % "3.0.1",
  "org.apache.spark" %% "spark-sql" % "3.0.1",
  "org.apache.commons" % "commons-math3" % "3.2",
  "com.github.scopt" %% "scopt" % "3.7.0",
  "org.apache.spark" %% "spark-mllib" % "3.0.1" % "provided"
)

// https://mvnrepository.com/artifact/org.neo4j/neo4j

libraryDependencies += "org.neo4j" % "neo4j-connector-apache-spark_2.12" % "4.0.2_for_spark_3"


//libraryDependencies += "log4j" % "log4j" % "1.2.14"
libraryDependencies ++= Seq(
  "org.apache.logging.log4j" %% "log4j-api-scala" % "11.0",
  "org.apache.logging.log4j" % "log4j-api" % "2.11.0",
  "org.apache.logging.log4j" % "log4j-core" % "2.11.0" % Runtime
)

//lazy val root = (project in file(".")).dependsOn(sustainDHT).settings()

//lazy val playJongo = RootProject(uri("https://github.com/bekce/play-jongo.git"))
//lazy val sustainDHT = RootProject(uri("https://github.com/Project-Sustain/synopsis-dht.git#master"))

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

test in assembly := {}