package org.sustain.clustering

import com.mongodb.spark.MongoSpark
import org.bson.Document
import org.apache.spark.ml.feature.{MinMaxScaler}
import org.apache.spark.mllib.linalg.{DenseVector, Matrix, Vectors}
import org.apache.spark.mllib.stat.Statistics

object DemoDBScanDriver {

  def main(args: Array[String]): Unit = {
    /* Create the SparkSession.
         * If config arguments are passed from the command line using --conf,
         * parse args for the values to set.
         */
    import org.apache.spark.sql.SparkSession
    println("Starting ...")

    case class GeoSpatial(features:DenseVector.type)

//    val spark = SparkSession.builder()
//      .master("spark://olympia:31865")
//      .appName("SparkClustering")
//      .config("spark.mongodb.input.uri", "mongodb://pierre.cs.colostate.edu:27023/noaadb2.noaa_test2")
//      .config("spark.mongodb.output.uri", "mongodb://pierre.cs.colostate.edu:27023/noaadb2.noaa_test2")
//      .getOrCreate()

        val spark = SparkSession.builder()
          .master("spark://menuka-HP:7077")
          .appName("SparkClustering")
          .config("spark.mongodb.input.uri", "mongodb://localhost:27017/sustaindb.hospitals_geo")
          .getOrCreate()

    val sc = spark.sparkContext

//    val df = MongoSpark.loadAndInferSchema(spark)
//    df.printSchema()
//    df.show()

    val rdd = MongoSpark.load(sc)
//    val new_rdd = rdd.map(d => d.get("properties").asInstanceOf[Document]).take(1)
    rdd.take(2).foreach(println)

//    val new_rdd = rdd.map(doc => doc.get("data").asInstanceOf[Document]).
//      map(d=> d.get("Latitude") + "," + d.get("Longitude") + "," + d.get("Temperature") + "," + d.get("Precipitation") + "," + d.get("Humidity"))

//////    val new_rdd = rdd.map(doc => doc.get("data").asInstanceOf[Document]).
//////      map(d=> {
//////
//////        val arr = Array(
//////            toDouble(d.get("Latitude").toString)
//////          , toDouble(d.get("Longitude").toString)
//////          , toDouble(d.get("Temperature").toString)
//////          , toDouble(d.get("Precipitation").toString)
//////          , toDouble(d.get("Humidity").toString)
//////        )
//////        Vectors.dense(arr)
//////      })
//////
//////    val df = spark.createDataFrame(new_rdd, GeoSpatial.getClass)
//////
//////    print("Soon after converting to Vectors")
//////    print(df.take(1))
//////
//////    //create a MinMaxScaler object
//////    val feature_scaler = new MinMaxScaler()
//////    feature_scaler.setInputCol("features")
//////    feature_scaler.setOutputCol("sfeatures")
//////
//////    // normalize features
//////    val smodel = feature_scaler.fit(df)
//////    val s_features_df = smodel.transform(df)
//////
//////    print("After fitting and transforming")
//////    print(s_features_df.take(1))
//////
//////    print("Show 10 items")
//////    print(s_features_df.select("features", "sfeatures").take(10))
//////
//////    val normalized_df = s_features_df.filter("sfeatures")
//////
//////    val normalized_rdd = normalized_df.rdd.map(row =>
//////      {
//////        val arr = Array(
//////          row.getDouble(0),
//////          row.getDouble(1),
//////          row.getDouble(2),
//////          row.getDouble(3),
//////          row.getDouble(4)
//////        )
//////        Vectors.dense(arr)
//////      })
//////
//////    print("Dataframe to RDD, 1 record")
//////    print(normalized_rdd.take(1))
////
////    // calculate the correlation matrix using Pearson's method. Use "spearman" for Spearman's method.
////    // If a method is not specified, Pearson's method will be used by default.
////    val correlMatrix: Matrix = Statistics.corr(normalized_rdd, "pearson")
//
//    correlMatrix.toString()
  }

  // function to convert year and incident_code columns to int
  def toDouble(value:String) : Double = {
    value.toDouble
  }
}
