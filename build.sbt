name := "org.sustain-clustering"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.mongodb.spark" %% "mongo-spark-connector" % "2.1.6",
  "org.apache.spark" %% "spark-core" % "2.1.3",
  "org.apache.spark" %% "spark-sql" % "2.1.3"
)