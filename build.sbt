import sbt.Keys.libraryDependencies

name := "sustain-clustering"

version := "0.0.4"

scalaVersion := "2.11.8"

resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

libraryDependencies ++= Seq(
  "org.mongodb.spark" %% "mongo-spark-connector" % "2.1.6",
  "org.apache.spark" %% "spark-core" % "2.2.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.1.3",
  "org.apache.commons" % "commons-math3" % "3.2",
  "com.github.scopt" %% "scopt" % "3.7.0",
    // https://mvnrepository.com/artifact/org.apache.spark/spark-mllib
  "org.apache.spark" %% "spark-mllib" % "2.2.1"

)

//lazy val root = (project in file(".")).dependsOn(sustainDHT).settings()

//lazy val playJongo = RootProject(uri("https://github.com/bekce/play-jongo.git"))
//lazy val sustainDHT = RootProject(uri("https://github.com/Project-Sustain/synopsis-dht.git#master"))

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

test in assembly := {}