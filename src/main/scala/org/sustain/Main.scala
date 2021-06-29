package org.sustain

import java.io.{File, FileWriter, PrintWriter}
import java.time.LocalDateTime

import com.mongodb.spark.MongoSpark
import org.apache.spark.ml.feature._
import org.apache.spark.sql.{Dataset, Row}
import SparkManager.logEnv
import org.sustain.clustering.KMeansClustering
import org.sustain.datasets.Features
import org.sustain.pcaClustering.PCAUtil
import org.sustain.util.Logger

import scala.collection.mutable.ArrayBuffer

object Main {

  def main(args: Array[String]): Unit = {
    logEnv()
    System.setProperty("mongodb.keep_alive_ms", "100000")
    // add new line to log file to indicate new invocation of the method
   Logger.log("-------------------------------------------------------------------------\n")
    /* Create the SparkSession.
   cd  * If config arguments are passed from the command line using --conf,
         * parse args for the values to set.
         */
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
    featureDF.take(5).foreach(i => Logger.log(i.toString()))

    // normalize data columns
    val normalizer = new Normalizer().setInputCol("features")

    val time1 = System.currentTimeMillis()

    val assembler = new VectorAssembler().setInputCols(features).setOutputCol("features")
    val featureDf = assembler.transform(featureDF)
    featureDf.show(10)

    // scaling
    Logger.log("Scaling features ...")
    val minMaxScaler: MinMaxScaler = new MinMaxScaler()
      .setInputCol("features")
      .setOutputCol("normalized_features")

    val scalerModel = minMaxScaler.fit(featureDf)

    var scaledDF = scalerModel.transform(featureDf)

    scaledDF = scaledDF.drop("features").withColumnRenamed("normalized_features", "features")
    Logger.log("Scaled DataFrame")
    scaledDF.show(10)
    KMeansClustering.runClustering(scaledDF, 56, features, collection1)

    // PCA
//    val pca: PCAModel = new PCA()
//      .setInputCol("features")
//      .setOutputCol("pcaFeatures")
//      .setK(13)
//      .fit(scaledDF)
//
//    val requiredNoOfPCs = PCAUtil.getNoPrincipalComponentsByVariance(pca, 0.95)
//    Logger.log("Collection " + collection1 + ", Required no. of PCs for 95% variability: " + requiredNoOfPCs)
//
//    var pcaDF: Dataset[Row] = pca.transform(scaledDF).select(Constants.GIS_JOIN, "features", "pcaFeatures")
//    pcaDF.show(20)

    Logger.log(s"Time taken: ${(System.currentTimeMillis() - time1) / 1000} seconds")
  }
}
