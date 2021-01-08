package org.sustain.clustering

import com.mongodb.spark.MongoSpark
import org.apache.spark.mllib.linalg.DenseVector

object DemoDBScanDriver {

  def main(args: Array[String]): Unit = {
    /* Create the SparkSession.
         * If config arguments are passed from the command line using --conf,
         * parse args for the values to set.
         */
    import org.apache.spark.sql.SparkSession
    println("Starting ...")

    case class GeoSpatial(features: DenseVector.type)

    val spark = SparkSession.builder()
      .master("spark://menuka-HP:7077")
      .appName("SparkClustering")
      .config("spark.mongodb.input.uri", "mongodb://localhost:27017/sustaindb.hospitals_geo")
      .getOrCreate()

    val sc = spark.sparkContext


    println("Fetching Hospitals ...")
    val hospitals = MongoSpark.load(sc)
    //    val new_rdd = rdd.map(d => d.get("properties").asInstanceOf[Document]).take(1)
    println(hospitals.count)
    println(hospitals.first.toJson)

    println("Fetching Dams ...")

    import com.mongodb.spark.config._
    val readConfig = ReadConfig(Map("collection" -> "dams_geo", "readPreference.name" -> "secondaryPreferred"), Some(ReadConfig(sc)))
    val customRdd = MongoSpark.load(sc, readConfig)
    println(customRdd.count)
    println(customRdd.first.toJson)

  }
}
