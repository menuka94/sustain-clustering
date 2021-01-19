package org.sustain.clustering

import com.mongodb.spark.MongoSpark
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.mllib.linalg.{DenseVector, Vectors}
import org.bson.Document

import java.io._
import java.time.LocalDateTime

object SustainClustering {
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

    //    val features1 = Array("properties.POPULATION", "properties.BEDS")
    val features1 = Array("BEDS", "POPULATION")
    val features2 = Array("properties.ENROLLMENT", "properties.FT_TEACHER")

    log("Collections: " + "[" + collection1 + ", " + collection2 + "]")
    log("Features: [" + features1 + ", " + features2 + "]")

//    case class GeoSpatial(features: DenseVector.type)
    case class Collection1(features: DenseVector.type)

    val spark = SparkSession.builder()
      .master("spark://menuka-HP:7077")
      .appName("SparkClustering")
      .config("spark.mongodb.input.uri", "mongodb://localhost:27017/sustaindb." + collection1)
      .getOrCreate()

    val sc = spark.sparkContext

    log("Fetching " + collection1 + " ...")
    val rdd1 = MongoSpark.load(sc)
    val new_rdd1 = rdd1.map(doc => doc.get("properties").asInstanceOf[Document])
      .map(d => {
        val arr = Array(
          toDouble(d.get("POPULATION").toString),
          toDouble(d.get("BEDS").toString)
        )
        Vectors.dense(arr)
      })

//    new_rdd1.take(5).foreach(i => log(i.toString))

    val df1 = spark.createDataFrame(new_rdd1, Collection1.getClass)

    df1.printSchema()
    df1.take(2).foreach(i => log(i.toString()))
    df1.show()


    // K-Means
//    /*
        val assembler = new VectorAssembler().setOutputCol("features")
        val featureDf = assembler.transform(df1)

        val kmeans = new KMeans().setK(2).setSeed(1L)
        val model = kmeans.fit(featureDf)

        log("Cluster centers ...")
        model.clusterCenters.foreach(println)
//    */

    //    log("Fetching " + collection2 + " ...")
    //
    //    import com.mongodb.spark.config._
    //
    //    val readConfig = ReadConfig(Map("collection" -> collection2, "readPreference.name" -> "secondaryPreferred"), Some(ReadConfig(sc)))
    //    val rdd2 = MongoSpark.load(sc, readConfig)
    //    val df2 = MongoSpark.load(spark, readConfig)
    //
    //    log("df2.getClass: " + df2.getClass)
    //    df2.printSchema()
    //    pw.close()


  }

  def log(message: String) {
    val log = LocalDateTime.now() + ": " + message
    println(log)
    pw.write(log + "\n")
    pw.flush()
  }

  // function to convert year and incident_code columns to int
  def toDouble(value: String): Double = {
    value.toDouble
  }
}
