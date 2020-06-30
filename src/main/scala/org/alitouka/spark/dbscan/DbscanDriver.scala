package org.alitouka.spark.dbscan

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.WriteConfig
import org.alitouka.spark.dbscan.spatial.rdd.PartitioningSettings
import org.alitouka.spark.dbscan.util.commandLine._
import org.alitouka.spark.dbscan.util.debug.{Clock, DebugHelper}
import org.alitouka.spark.dbscan.util.io.IOHelper

/** A driver program which runs DBSCAN clustering algorithm
 *
 */
object DbscanDriver {

  private [dbscan] class Args (var minPts: Int = DbscanSettings.getDefaultNumberOfPoints,
      var borderPointsAsNoise: Boolean = DbscanSettings.getDefaultTreatmentOfBorderPoints)
      extends CommonArgs with EpsArg with NumberOfPointsInPartitionArg

  private [dbscan] class ArgsParser
    extends CommonArgsParser (new Args (), "DBSCAN clustering algorithm")
    with EpsArgParsing [Args]
    with NumberOfPointsInPartitionParsing [Args] {

    opt[Int] ("numPts")
      .required()
      .foreach { args.minPts = _ }
      .valueName("<minPts>")
      .text("TODO: add description")

    opt[Boolean] ("borderPointsAsNoise")
      .foreach { args.borderPointsAsNoise = _ }
      .text ("A flag which indicates whether border points should be treated as noise")
  }


  def main (args: Array[String]): Unit = {

    val argsParser = new ArgsParser ()

    if (argsParser.parse (args)) {

      val clock = new Clock ()


      import org.apache.spark.sql.SparkSession

      val spark = SparkSession.builder()
        .master(argsParser.args.masterUrl)
        .appName("SparkClustering")
        .config("spark.jars", Array(argsParser.args.jar).toSeq.filter(_ != null).mkString(","))
        .config("spark.mongodb.input.uri", "mongodb://pierre.cs.colostate.edu:27023/noaadb2.noaa_test2")
        .config("spark.mongodb.output.uri", "mongodb://pierre.cs.colostate.edu:27023/noaadb2.noaa_test2_cluster")
        .getOrCreate()

      if (argsParser.args.debugOutputPath.isDefined) {
        spark.conf.set (DebugHelper.DebugOutputPath, argsParser.args.debugOutputPath.get)
      }

      val sc = spark.sparkContext

      val mongoRDD = MongoSpark.load(sc)

      val data = IOHelper.readMongoRDD(sc, mongoRDD)

      //data.foreach(pt => println("In DbscanDriver " + pt.orgPointId))

      val settings = new DbscanSettings ()
        .withEpsilon(argsParser.args.eps)
        .withNumberOfPoints(argsParser.args.minPts)
        .withTreatBorderPointsAsNoise(argsParser.args.borderPointsAsNoise)
        .withDistanceMeasure(argsParser.args.distanceMeasure)

      val partitioningSettings = new PartitioningSettings (numberOfPointsInBox = argsParser.args.numberOfPoints)

      val clusteringResult = Dbscan.train(data, settings, partitioningSettings)

      //TODO change the shardkey accordingly
      val writeConfig = WriteConfig(Map( "shardkey" -> "{_id: \"hashed\"}","writeConcern.w" -> "majority"), Some(WriteConfig(sc)))

      IOHelper.saveClusteringResultInMongoDB(clusteringResult, writeConfig)
      clock.logTimeSinceStart("Clustering")
    }
  }
}
