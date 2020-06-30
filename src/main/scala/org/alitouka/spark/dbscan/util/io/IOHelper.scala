package org.alitouka.spark.dbscan.util.io

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.WriteConfig
import com.mongodb.spark.rdd.MongoRDD
import org.alitouka.spark.dbscan.spatial.Point
import org.alitouka.spark.dbscan.{DbscanModel, RawDataSet}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bson.Document

/** Contains functions for reading and writing data
  *
  */
object IOHelper {

  /**
   * Reads data from MongoRDD and converts to RawDataSet
   * @param sc A SparkContext into which the data should be loaded
   * @param rdd MongoRDD which consists of data read from MongoDB
   * @return A [[org.alitouka.spark.dbscan.RawDataSet]] populated with points
   */
  def readMongoRDD(sc: SparkContext, rdd: MongoRDD[Document]): RawDataSet = {

    val raw = rdd.map(doc => doc.get("data").asInstanceOf[Document]).map( d =>
      d.get("Latitude") + valSeparator + d.get("Longitude") + valSeparator + d.get("Temperature") + valSeparator + d.get("Precipitation") + valSeparator + d.get("Humidity"))

    val zippedRDD = raw.zipWithIndex()

    zippedRDD.map (
      line => {
        new Point (line._1.split(valSeparator).map( _.toDouble ), line._2)
      }
    )
  }

  /** Reads a dataset from a CSV file. That file should contain double values separated by commas
    *
    * @param sc A SparkContext into which the data should be loaded
    * @param path A path to the CSV file
    * @return A [[org.alitouka.spark.dbscan.RawDataSet]] populated with points
    */
  def readDataset (sc: SparkContext, path: String): RawDataSet = {
    val rawData = sc.textFile (path)

    rawData.map (
      line => {
        new Point (line.split(idSeparator)(1).split(separator).map( _.toDouble ), line.split(idSeparator)(0).toLong)
      }
    )
  }

  /** Saves clustering result into a CSV file. The resulting file will contain the same data as the input file,
    * with a cluster ID appended to each record. The order of records is not guaranteed to be the same as in the
    * input file
    *
    * @param model A [[org.alitouka.spark.dbscan.DbscanModel]] obtained from Dbscan.train method
    * @param outputPath Path to a folder where results should be saved. The folder will contain multiple
    *                   partXXXX files
    */
  def saveClusteringResult (model: DbscanModel, outputPath: String) {

    val sortedModel = model.allPoints.sortBy(pt => pt.orgPointId, ascending = true)

    sortedModel.map ( pt => {

      pt.orgPointId+ idSeparator + pt.coordinates.mkString(separator) + separator + pt.clusterId
    } ).saveAsTextFile(outputPath)
  }

  private [dbscan] def saveTriples (data: RDD[(Double, Double, Long)], outputPath: String) {
    data.map ( x => x._1 + separator + x._2 + separator + x._3 ).saveAsTextFile(outputPath)
  }

  private def idSeparator = ""
  private def separator = ","
  private def valSeparator = "~"

  /**
   * Saves the points along with the clusterID in MongoDB collection.
   * @param model Clustering model
   * @param writeConfig Specifies different write configs including shard key for collection
   */
  def saveClusteringResultInMongoDB (model: DbscanModel, writeConfig: WriteConfig) {

    println("In save clustering to MongoDB")

    val sortedModel = model.allPoints.sortBy(pt => pt.orgPointId, ascending = true)

    val documents = sortedModel.map ( pt => {

      Document.parse(s"{data: " +
        s"{Latitude : ${pt.coordinates(0)}," +
        s" Longitude : ${pt.coordinates(1)}, " +
        s" Temperature : ${pt.coordinates(2)}, " +
        s" Precipitation : ${pt.coordinates(3)}, " +
        s" Humidity : ${pt.coordinates(4)}}, " +
        s"clusterId: { '1' : ${pt.clusterId} }}")
    } )

    MongoSpark.save(documents, writeConfig)
  }

}
