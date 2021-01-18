package org.sustain.clustering

import com.mongodb.spark.MongoSpark
import org.apache.spark.mllib.linalg.DenseVector

import java.io._
import java.time.LocalDateTime

object DemoDBScanDriver {
  val logFile: String = System.getenv("HOME") + "/sustain-clustering.log"
  val pw: PrintWriter = new PrintWriter(new FileWriter(new File(logFile), true))

  def main(args: Array[String]): Unit = {
    // add new line to log file to indicate new invocation of the method
    pw.write("-------------------------------------------------------------------------\n")
    /* Create the SparkSession.
         * If config arguments are passed from the command line using --conf,
         * parse args for the values to set.
         */
    import org.apache.spark.sql.SparkSession
    println("Starting ...")

    // TODO: populate the following with user inputs
    val collection1 = "hospitals_geo"
    val collection2 = "public_schools"

    val features1 = Array("properties.population", "properties.BEDS")
    val features2 = Array("properties.ENROLLMENT", "properties.FT_TEACHER")

    log("Collections: " + "[" + collection1 + ", " + collection2 + "]")
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
    val df1 = MongoSpark.load(spark)

    log("rdd1.getClass: " + rdd1.getClass)
    log("df1.getClass: " + df1.getClass)

    log("Fetching " + collection2 + " ...")

    import com.mongodb.spark.config._

    val readConfig = ReadConfig(Map("collection" -> collection2, "readPreference.name" -> "secondaryPreferred"), Some(ReadConfig(sc)))
    val rdd2 = MongoSpark.load(sc, readConfig)
    println(rdd2.count)
    println(rdd2.first.toJson)
    pw.close()
  }

  def log(message: String) {
    val log = LocalDateTime.now() + ": " + message
    println(log)
    pw.write(log + "\n")
    pw.flush()
  }
}
