package org.sustain.clustering

import com.mongodb.spark.MongoSpark
import org.apache.spark.ml.feature._
import org.apache.spark.sql.functions.{avg, col}
import org.apache.spark.sql.{Dataset, Row}
import java.io._
import java.time.LocalDateTime

import org.sustain.clustering.SparkManager.logEnv

import scala.collection.mutable.ArrayBuffer

object SustainClustering {
  val logFile: String = System.getenv("HOME") + "/sustain-clustering.log"
  val pw: PrintWriter = new PrintWriter(new FileWriter(new File(logFile), true))

  def main(args: Array[String]): Unit = {
    logEnv()
    System.setProperty("mongodb.keep_alive_ms", "100000")
    // add new line to log file to indicate new invocation of the method
    pw.write("-------------------------------------------------------------------------\n")
    /* Create the SparkSession.
         * If config arguments are passed from the command line using --conf,
         * parse args for the values to set.
         */
    import org.apache.spark.sql.SparkSession
    println("Starting ...")

    val collection1 = "svi_county_GISJOIN"

    val spark = SparkManager.getSparkSession(collection1);

    val sc = spark.sparkContext

    import com.mongodb.spark.config._

    // fetch data
    var featureDF = MongoSpark.load(spark,
      ReadConfig(Map("collection" -> collection1, "readPreference.name" -> "secondaryPreferred"), Some(ReadConfig(sc))))

    //    featureDF = featureDF.sample(0.5)
    var features = Features.sviFeatures

    val featuresWithGisJoin: ArrayBuffer[String] = ArrayBuffer(features: _*)
    featuresWithGisJoin += Constants.GIS_JOIN
    featureDF = featureDF.select(featuresWithGisJoin.head, featuresWithGisJoin.tail: _*);
    featureDF.printSchema()
    featureDF.take(5).foreach(i => log(i.toString()))

    // normalize data columns
    val normalizer = new Normalizer().setInputCol("features")

    val time1 = System.currentTimeMillis()

    val assembler = new VectorAssembler().setInputCols(features).setOutputCol("features")
    val featureDf = assembler.transform(featureDF)
    featureDf.show(10)

    // scaling
    log("Scaling features ...")
    val minMaxScaler: MinMaxScaler = new MinMaxScaler()
      .setInputCol("features")
      .setOutputCol("normalized_features")

    val scalerModel = minMaxScaler.fit(featureDf)

    var scaledDF = scalerModel.transform(featureDf)

    scaledDF = scaledDF.drop("features").withColumnRenamed("normalized_features", "features")
    log("Scaled DataFrame")
    scaledDF.show(10)

    // PCA
    val pca: PCAModel = new PCA()
      .setInputCol("features")
      .setOutputCol("pcaFeatures")
      .setK(13)
      .fit(scaledDF)

    val requiredNoOfPCs = PCAUtil.getNoPrincipalComponentsByVariance(pca, 0.95)
    log("Collection " + collection1 + ", Required no. of PCs for 95% variability: " + requiredNoOfPCs)

    var pcaDF: Dataset[Row] = pca.transform(scaledDF).select(Constants.GIS_JOIN, "features", "pcaFeatures")
    pcaDF.show(20)

    log(s"Time taken: ${(System.currentTimeMillis() - time1)/1000} seconds")
  }

  def log(message: String) {
    val log = LocalDateTime.now() + ": " + message
    println(log)
    pw.write(log + "\n")
    pw.flush()
  }
}
