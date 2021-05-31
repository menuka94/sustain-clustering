package org

import org.sustain.clustering.Constants

object Neo4JConnect {

  def main(args: Array[String]): Unit = {

    import org.apache.spark.sql.SparkSession
    println("Starting ...")

    val spark = SparkSession.builder()
      .master(Constants.SPARK_MASTER)
      .appName(s"Neo4j")
      .getOrCreate()

    val sc = spark.sparkContext

  }
}
