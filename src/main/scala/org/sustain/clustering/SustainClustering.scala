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
      .appName(s"Clustering ('$collection1'): with original features")
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
    scaledDF = scaledDF.groupBy("gis_join").agg(
      avg("year_month_day_hour").as("avg_year_month_day_hour"),
      avg("mean_sea_level_pressure_pascal").as("avg_mean_sea_level_pressure_pascal"),
      avg("surface_pressure_surface_level_pascal").as("avg_surface_pressure_surface_level_pascal"),
      avg("orography_surface_level_meters").as("avg_orography_surface_level_meters"),
      avg("temp_surface_level_kelvin").as("avg_temp_surface_level_kelvin"),
      avg("2_metre_temp_kelvin").as("avg_2_metre_temp_kelvin"),
      avg("2_metre_dewpoint_temp_kelvin").as("avg_2_metre_dewpoint_temp_kelvin"),
      avg("relative_humidity_percent").as("avg_relative_humidity_percent"),
      avg("10_metre_u_wind_component_meters_per_second").as("avg_10_metre_u_wind_component_meters_per_second"),
      avg("10_metre_v_wind_component_meters_per_second").as("avg_10_metre_v_wind_component_meters_per_second"),
      avg("total_precipitation_kg_per_squared_meter").as("avg_total_precipitation_kg_per_squared_meter"),
      avg("water_convection_precipitation_kg_per_squared_meter").as("avg_water_convection_precipitation_kg_per_squared_meter"),
      avg("soil_temperature_kelvin").as("avg_soil_temperature_kelvin"),
      avg("pressure_pascal").as("avg_pressure_pascal"),
      avg("visibility_meters").as("avg_visibility_meters"),
      avg("precipitable_water_kg_per_squared_meter").as("avg_precipitable_water_kg_per_squared_meter"),
      avg("total_cloud_cover_percent").as("avg_total_cloud_cover_percent"),
      avg("snow_depth_meters").as("avg_snow_depth_meters"),
      avg("ice_cover_binary").as("avg_ice_cover_binary")
    )

    log("Scaled DataFrame")
    scaledDF.show(10)

    features = Array(
      "avg_year_month_day_hour",
      "avg_mean_sea_level_pressure_pascal",
      "avg_surface_pressure_surface_level_pascal",
      "avg_orography_surface_level_meters",
      "avg_temp_surface_level_kelvin",
      "avg_2_metre_temp_kelvin",
      "avg_2_metre_dewpoint_temp_kelvin",
      "avg_relative_humidity_percent",
      "avg_10_metre_u_wind_component_meters_per_second",
      "avg_10_metre_v_wind_component_meters_per_second",
      "avg_total_precipitation_kg_per_squared_meter",
      "avg_water_convection_precipitation_kg_per_squared_meter",
      "avg_soil_temperature_kelvin",
      "avg_pressure_pascal",
      "avg_visibility_meters",
      "avg_precipitable_water_kg_per_squared_meter",
      "avg_total_cloud_cover_percent",
      "avg_snow_depth_meters",
      "avg_ice_cover_binary"
    )

    KMeansClustering.runClustering(spark, scaledDF, features, 56, collection1)
    BisectingKMeansClustering.runClustering(spark, scaledDF, features, 56, collection1)
    GaussianMixtureClustering.runClustering(spark, scaledDF, features, 56, collection1)
  }

  def log(message: String) {
    val log = LocalDateTime.now() + ": " + message
    println(log)
    pw.write(log + "\n")
    pw.flush()
  }
}
