package org.sustain.clustering

import com.mongodb.spark.MongoSpark
import org.bson.Document

object DemoDBScanDriver {

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

    //val df = MongoSpark.loadAndInferSchema(spark)
    //df.printSchema()
    //df.show()

    val rdd = MongoSpark.load(sc)
    val new_rdd = rdd.map(doc => doc.get("data").asInstanceOf[Document]).
      map(d=> d.get("Latitude") + "," + d.get("Longitude") + "," + d.get("Temperature") + "," + d.get("Precipitation") + "," + d.get("Humidity"))
    //println(rdd.count)
    // println(new_rdd.first.toString)


    //println(rdd.count)
    println(new_rdd.first)
  }

}
