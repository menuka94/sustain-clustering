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
    log("SERVER_HOST: " + Constants.SERVER_HOST)
    log("SERVER_PORT: " + Constants.SERVER_PORT)
    log("DB_HOST: " + Constants.DB_HOST)
    log("DB_PORT: " + Constants.DB_PORT)
  }

  def main(args: Array[String]): Unit = {
    logEnv()
    // add new line to log file to indicate new invocation of the method
    pw.write("-------------------------------------------------------------------------\n")
    /* Create the SparkSession.
         * If config arguments are passed from the command line using --conf,
         * parse args for the values to set.
         */
    import org.apache.spark.sql.SparkSession
    println("Starting ...")

    // TODO: populate the following with user inputs
    val collection1 = "svi_county_GISJOIN"

    val spark = SparkSession.builder()
      .master(Constants.SPARK_MASTER)
      .appName("SparkClustering")
      .config("spark.mongodb.input.uri",
        "mongodb://" + Constants.DB_HOST + ":" + Constants.DB_PORT + "/sustaindb." + collection1)
      .getOrCreate()

    val sc = spark.sparkContext

    import com.mongodb.spark.config._

    // fetch data
    var featureDF = MongoSpark.load(spark,
      ReadConfig(Map("collection" -> "svi_county_GISJOIN", "readPreference.name" -> "secondaryPreferred"), Some(ReadConfig(sc))))

    var features = Features.sviFeatures

    val featuresWithGisJoin: ArrayBuffer[String]= ArrayBuffer(features: _*)
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
      .setK(features.length)
      .fit(scaledDF)

    val requiredNoOfPCs = PCAUtil.getNoPrincipalComponentsByVariance(pca, .95)
    log("Collection " + collection1 + ", Required no. of PCs for 95% variability: " + requiredNoOfPCs)

    var pcaDF: Dataset[Row] = pca.transform(scaledDF).select(Constants.GIS_JOIN, "features", "pcaFeatures")
    pcaDF.show(20)

    val disassembler = new VectorDisassembler().setInputCol("pcaFeatures")
    pcaDF = disassembler.transform(pcaDF)

    pcaDF.show(20)

    // average principal components
    pcaDF = pcaDF.groupBy(col(Constants.GIS_JOIN)).agg(
      avg("pcaFeatures_0").as("avg_pc_0"),
      avg("pcaFeatures_1").as("avg_pc_1"),
      avg("pcaFeatures_2").as("avg_pc_2"),
      avg("pcaFeatures_3").as("avg_pc_3"),
      avg("pcaFeatures_4").as("avg_pc_4"),
      avg("pcaFeatures_5").as("avg_pc_5"),
      avg("pcaFeatures_6").as("avg_pc_6"),
      avg("pcaFeatures_7").as("avg_pc_7"),
      avg("pcaFeatures_8").as("avg_pc_8"),
    ).
      select(Constants.GIS_JOIN,
        "avg_pc_0",
        "avg_pc_1",
        "avg_pc_2",
        "avg_pc_3",
        "avg_pc_4",
        "avg_pc_5",
        "avg_pc_6",
        "avg_pc_7",
        "avg_pc_8"
      )


    // KMeans Clustering
    KMeansClustering.runClustering(spark,
      pcaDF,
      Array(
        "avg_pc_1",
        "avg_pc_2",
        "avg_pc_3",
        "avg_pc_4",
        "avg_pc_5",
        "avg_pc_6",
        "avg_pc_7",
        "avg_pc_8"
      ),
      56,
      collection1
    )
  }

  def log(message: String) {
    val log = LocalDateTime.now() + ": " + message
    println(log)
    pw.write(log + "\n")
    pw.flush()
  }
}
