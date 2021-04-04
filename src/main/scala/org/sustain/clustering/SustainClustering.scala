package org.sustain.clustering

import com.mongodb.spark.MongoSpark
import org.apache.spark.ml.feature.{Normalizer, StandardScaler, VectorAssembler}

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
    val collection1 = "svi_county_GISJOIN"

    val spark = SparkSession.builder()
      .master("spark://menuka-HP:7077")
      .appName("SparkClustering")
      .config("spark.mongodb.input.uri", "mongodb://localhost:27017/sustaindb." + collection1)
      .getOrCreate()

    val sc = spark.sparkContext

    import com.mongodb.spark.config._

    // fetch data
    var county_stats = MongoSpark.load(spark,
      ReadConfig(Map("collection" -> "svi_county_GISJOIN", "readPreference.name" -> "secondaryPreferred"), Some(ReadConfig(sc))))

    var features = Array(
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


    county_stats = county_stats.select(features.head, features.tail: _*);
    county_stats.printSchema()
    county_stats.take(5).foreach(i => log(i.toString()))

    // normalize data columns
    val normalizer = new Normalizer().setInputCol("features")

    val assembler = new VectorAssembler().setInputCols(features.patch(0, Nil, 1)).setOutputCol("features")
    val featureDf = assembler.transform(county_stats)
    featureDf.show(10)

    // scaling
    log("Scaling features ...")
    var scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaled_features")
      .setWithMean(true)
      .setWithMean(false)

    val scalerModel = scaler.fit(featureDf)

    val scaledDF = scalerModel.transform(featureDf)
    log("Scaled DataFrame")
    scaledDF.show(10)

  }

  def log(message: String) {
    val log = LocalDateTime.now() + ": " + message
    println(log)
    pw.write(log + "\n")
    pw.flush()
  }
}
