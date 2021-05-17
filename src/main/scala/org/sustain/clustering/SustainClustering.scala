package org.sustain.clustering

import com.mongodb.spark.MongoSpark
import org.apache.spark.ml.feature._
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

    var features = Array[String](
      "E_HU",
      "M_HU",
      "E_HH",
      "M_HH",
      "E_POV",
      "M_POV",
      "E_UNEMP",
      "M_UNEMP",
      "E_PCI",
      "M_PCI",
      "E_NOHSDP",
      "M_NOHSDP",
      "E_AGE65",
      "M_AGE65",
      "E_AGE17",
      "M_AGE17",
      "E_DISABL",
      "M_DISABL",
      "E_SNGPNT",
      "M_SNGPNT",
      "E_MINRTY",
      "M_MINRTY",
      "E_LIMENG",
      "M_LIMENG",
      "E_MUNIT",
      "M_MUNIT",
      "E_MOBILE",
      "M_MOBILE",
      "E_CROWD",
      "M_CROWD",
      "E_NOVEH",
      "M_NOVEH",
      "E_GROUPQ",
      "M_GROUPQ"
    )

    val featuresWithGisJoin: ArrayBuffer[String]= ArrayBuffer(features: _*)
    featuresWithGisJoin += "GISJOIN"
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

    var pcaDF: Dataset[Row] = pca.transform(scaledDF).select("GISJOIN", "features", "pcaFeatures")
    pcaDF.show(20)

    val disassembler = new VectorDisassembler().setInputCol("pcaFeatures")
    pcaDF = disassembler.transform(pcaDF)

    pcaDF.show(20)


    // KMeans Clustering
  }

  def log(message: String) {
    val log = LocalDateTime.now() + ": " + message
    println(log)
    pw.write(log + "\n")
    pw.flush()
  }
}
