package org.sustain.clustering

import com.mongodb.spark.MongoSpark
import org.apache.spark.ml.feature._
import org.apache.spark.sql.functions.{avg, col}
import org.apache.spark.sql.{Dataset, Row}

import java.io._
import java.time.LocalDateTime
import scala.collection.mutable.ArrayBuffer

object SustainClustering {
  val logFile: String = System.getenv("HOME") + "/sustain-clustering.log"
  val pw: PrintWriter = new PrintWriter(new FileWriter(new File(logFile), true))

  def logEnv(): Unit = {
    log(">>> Log Environment")
    log("SPARK_MASTER: " + Constants.SPARK_MASTER)
    log("DB_HOST: " + Constants.DB_HOST)
    log("DB_PORT: " + Constants.DB_PORT)
  }

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

    val collection1 = "noaa_nam_2"

    val spark = SparkSession.builder()
      .master(Constants.SPARK_MASTER)
      .appName(s"Clustering ('$collection1')")
      .config("spark.mongodb.input.uri",
        "mongodb://" + Constants.DB_HOST + ":" + Constants.DB_PORT + "/sustaindb." + collection1)
      .getOrCreate()

    val sc = spark.sparkContext

    import com.mongodb.spark.config._

    // fetch data
    var featureDF = MongoSpark.load(spark,
      ReadConfig(Map("collection" -> collection1, "readPreference.name" -> "secondaryPreferred"), Some(ReadConfig(sc))))

//    featureDF = featureDF.sample(0.5)
    var features = Features.noaaFeatures

    val featuresWithGisJoin: ArrayBuffer[String] = ArrayBuffer(features: _*)
    featuresWithGisJoin += Constants.GIS_JOIN
    featureDF = featureDF.select(featuresWithGisJoin.head, featuresWithGisJoin.tail: _*);
    featureDF.printSchema()
    featureDF.take(5).foreach(i => log(i.toString()))

    // normalize data columns
    val normalizer = new Normalizer().setInputCol("features")

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
      .setK(18)
      .fit(scaledDF)

    val requiredNoOfPCs = PCAUtil.getNoPrincipalComponentsByVariance(pca, .95)
    log("Collection " + collection1 + ", Required no. of PCs for 95% variability: " + requiredNoOfPCs)

    var pcaDF: Dataset[Row] = pca.transform(scaledDF).select(Constants.GIS_JOIN, "features", "pcaFeatures")
    pcaDF.show(20)

    val disassembler = new VectorDisassembler().setInputCol("pcaFeatures")
    pcaDF = disassembler.transform(pcaDF)

    pcaDF.show(20)

    // average principal components
    val pcaDF_all = pcaDF.groupBy(col(Constants.GIS_JOIN)).agg(
      avg("pcaFeatures_0").as("avg_pc_0"),
      avg("pcaFeatures_1").as("avg_pc_1"),
      avg("pcaFeatures_2").as("avg_pc_2"),
      avg("pcaFeatures_3").as("avg_pc_3"),
      avg("pcaFeatures_4").as("avg_pc_4"),
      avg("pcaFeatures_5").as("avg_pc_5"),
      avg("pcaFeatures_6").as("avg_pc_6"),
      avg("pcaFeatures_7").as("avg_pc_7"),
      avg("pcaFeatures_8").as("avg_pc_8"),
      avg("pcaFeatures_9").as("avg_pc_9"),
      avg("pcaFeatures_10").as("avg_pc_10"),
      avg("pcaFeatures_11").as("avg_pc_11"),
      avg("pcaFeatures_12").as("avg_pc_12"),
      avg("pcaFeatures_13").as("avg_pc_13"),
      avg("pcaFeatures_14").as("avg_pc_14"),
      avg("pcaFeatures_15").as("avg_pc_15"),
      avg("pcaFeatures_16").as("avg_pc_16"),
      avg("pcaFeatures_17").as("avg_pc_17")
    )

    val count = pcaDF.count()
    log(s"pcaDF: count = $count")


    // Clustering: 13 PCs
    val pcaDF13 = pcaDF_all.select(Constants.GIS_JOIN,
      "avg_pc_0", "avg_pc_1", "avg_pc_2", "avg_pc_3", "avg_pc_4", "avg_pc_5", "avg_pc_6", "avg_pc_7",
      "avg_pc_8", "avg_pc_9", "avg_pc_10", "avg_pc_11", "avg_pc_12"
    )

    KMeansClustering.runClustering(spark,
      pcaDF,
      Array(
        "avg_pc_0", "avg_pc_1", "avg_pc_2", "avg_pc_3", "avg_pc_4", "avg_pc_5", "avg_pc_6", "avg_pc_7", "avg_pc_8",
        "avg_pc_9", "avg_pc_10", "avg_pc_11", "avg_pc_12"
      ),
      56,
      13,
      collection1
    )

    // Clustering: 9 PCs
    val pcaDF9 = pcaDF_all.select(Constants.GIS_JOIN,
        "avg_pc_0", "avg_pc_1", "avg_pc_2", "avg_pc_3", "avg_pc_4", "avg_pc_5", "avg_pc_6", "avg_pc_7", "avg_pc_8"
      )

    KMeansClustering.runClustering(spark,
      pcaDF,
      Array(
        "avg_pc_0", "avg_pc_1", "avg_pc_2", "avg_pc_3", "avg_pc_4", "avg_pc_5", "avg_pc_6", "avg_pc_7", "avg_pc_8",
        "avg_pc_9", "avg_pc_10", "avg_pc_11", "avg_pc_12"
      ),
      56,
      13,
      collection1
    )


    // Clustering: 8 PCs
    val pcaDF8 = pcaDF_all.select(Constants.GIS_JOIN,
      "avg_pc_0", "avg_pc_1", "avg_pc_2", "avg_pc_3", "avg_pc_4", "avg_pc_5", "avg_pc_6", "avg_pc_7",
    )

    // Clustering: 7 PCs
    val pcaDF7 = pcaDF_all.select(Constants.GIS_JOIN,
      "avg_pc_0", "avg_pc_1", "avg_pc_2", "avg_pc_3", "avg_pc_4", "avg_pc_5", "avg_pc_6"
    )


    // Clustering: 6 PCs
    val pcaDF6 = pcaDF_all.select(Constants.GIS_JOIN,
      "avg_pc_0", "avg_pc_1", "avg_pc_2", "avg_pc_3", "avg_pc_4", "avg_pc_5"
    )


    // Clustering: 5 PCs
    val pcaDF5 = pcaDF_all.select(Constants.GIS_JOIN,
      "avg_pc_0", "avg_pc_1", "avg_pc_2", "avg_pc_3", "avg_pc_4"
    )


    // Clustering: 4 PCs
    val pcaDF4 = pcaDF_all.select(Constants.GIS_JOIN,
      "avg_pc_0", "avg_pc_1", "avg_pc_2", "avg_pc_3"
    )

  }

  def log(message: String) {
    val log = LocalDateTime.now() + ": " + message
    println(log)
    pw.write(log + "\n")
    pw.flush()
  }
}
