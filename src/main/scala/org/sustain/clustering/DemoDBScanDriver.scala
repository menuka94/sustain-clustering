package org.sustain.clustering

import com.mongodb.spark.MongoSpark
import org.apache.spark.mllib.linalg.DenseVector

import java.time.LocalDateTime

object DemoDBScanDriver {

  def main(args: Array[String]): Unit = {
    /* Create the SparkSession.
         * If config arguments are passed from the command line using --conf,
         * parse args for the values to set.
         */
    import org.apache.spark.sql.SparkSession
    println("Starting ...")

    // TODO: populate the following with user inputs
    val collection1 = "hospitals_geo"
    val collection2 = "public_schools"

    log("Collections: " + "[" + collection1 + ", " + collection2 + "]")

    log("FROM LOGGER")

    val features1 = Array("properties.population", "properties.BEDS")
    val features2 = Array("properties.ENROLLMENT", "properties.FT_TEACHER")

    log("Features: [" + features1 + ", " + features2 + "]")

    case class GeoSpatial(features: DenseVector.type)

    val spark = SparkSession.builder()
      .master("spark://menuka-HP:7077")
      .appName("SparkClustering")

      .config("spark.mongodb.input.uri", "mongodb://localhost:27017/sustaindb." + collection1)
      .getOrCreate()

    val sc = spark.sparkContext
    log("Fetching " + collection1 + " ...")
    val rdd1 = MongoSpark.load(sc)
    log("CLASS: " + rdd1.getClass)

    log("Fetching " + collection2 + " ...")

    import com.mongodb.spark.config._

    val readConfig = ReadConfig(Map("collection" -> collection2, "readPreference.name" -> "secondaryPreferred"), Some(ReadConfig(sc)))
    val rdd2 = MongoSpark.load(sc, readConfig)
    println(rdd2.count)
    println(rdd2.first.toJson)
  }

  def log(message: String) {
    println(LocalDateTime.now() + ": " + message)
  }
}
