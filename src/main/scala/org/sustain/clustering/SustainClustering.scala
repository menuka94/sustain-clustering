package org.sustain.clustering

import com.mongodb.spark.MongoSpark
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler

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
    val collection1 = "county_stats"

    //    val features1 = Array("properties.POPULATION", "properties.BEDS")
    val features1 = Array("total_population", "median_household_income")


    val spark = SparkSession.builder()
      .master("spark://menuka-HP:7077")
      .appName("SparkClustering")
      .config("spark.mongodb.input.uri", "mongodb://localhost:27017/sustaindb." + collection1)
      .getOrCreate()

    val sc = spark.sparkContext

    import com.mongodb.spark.config._
    import spark.implicits._

    var county_stats = MongoSpark.load(spark,
      ReadConfig(Map("collection" -> "county_stats", "readPreference.name" -> "secondaryPreferred"), Some(ReadConfig(sc))))

    county_stats = county_stats.select($"GISJOIN", $"total_population".cast("double"), $"median_household_income");
    county_stats.printSchema()
    county_stats.take(5).foreach(i => log(i.toString()))

    // K-Means
    val assembler = new VectorAssembler().setInputCols(Array("total_population", "median_household_income")).setOutputCol("features")
    val featureDf = assembler.transform(county_stats)

    val kmeans = new KMeans().setK(10).setSeed(1L)
    val model = kmeans.fit(featureDf)

    log("Cluster centers ...")
    model.clusterCenters.foreach(x => log(x.toString))

    val predictDf = model.transform(featureDf)
    predictDf.show(10)

  }

  def log(message: String) {
    val log = LocalDateTime.now() + ": " + message
    println(log)
    pw.write(log + "\n")
    pw.flush()
  }
}
