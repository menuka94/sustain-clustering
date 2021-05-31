package org

import org.sustain.clustering.Constants
import org.apache.spark.sql.{SaveMode, SparkSession};
import org.neo4j.spark._

object Neo4JConnect {

  def main(args: Array[String]): Unit = {

    import org.apache.spark.sql.SparkSession
    println("Starting ...")

    val spark = SparkSession.builder()
      .master(Constants.SPARK_MASTER)
      .appName(s"Neo4j Spark")
      .getOrCreate()

    val sc = spark.sparkContext

    val df = spark.read.format("org.neo4j.spark.DataSource")
      .option("url", "bolt://localhost:7687")
      .option("authentication.basic.username", "neo4j")
      .option("authentication.basic.password", "neo4j")
      .option("labels", "Person")
      .load()

    df.show()

  }
}
