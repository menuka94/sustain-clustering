package org.sustain.clustering

import com.mongodb.spark.MongoSpark

/**
 * Created by laksheenmendis on 6/25/20 at 4:38 PM
 */
class DBScanDriver {

  def main(args: Array[String]): Unit = {
    /* Create the SparkSession.
         * If config arguments are passed from the command line using --conf,
         * parse args for the values to set.
         */
    import org.apache.spark.sql.SparkSession

    val spark = SparkSession.builder()
      .master("spark://olympia:31865")
      .appName("SparkClustering")
      .config("spark.mongodb.input.uri", "mongodb://pierre.cs.colostate.edu:27023/noaadb2.noaa_test2")
      .config("spark.mongodb.output.uri", "mongodb://pierre.cs.colostate.edu:27023/noaadb2.noaa_test2")
      .getOrCreate()

    val sc = spark.sparkContext

    val rdd = MongoSpark.load(sc)

    println(rdd.count)
    println(rdd.first.toJson)
  }

}
