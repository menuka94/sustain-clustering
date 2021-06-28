package org.sustain.pcaClustering

import org.apache.spark.ml.clustering.BisectingKMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.sustain.util.Logger
import org.sustain.{Constants, Main}

object BisectingKMeansClustering {
  def runClustering(spark: SparkSession,
                    inputCollection: Dataset[Row],
                    clusteringFeatures: Array[String],
                    clusteringK: Int,
                    noOfPCs: Int,
                    collectionName: String): Unit = {

    var clusteringCollection = inputCollection

    val assembler: VectorAssembler = new VectorAssembler()
      .setInputCols(clusteringFeatures)
      .setOutputCol("features")

    val featureDF: Dataset[Row] = assembler.transform(clusteringCollection).select(Constants.GIS_JOIN, "features")

    val bkm = new BisectingKMeans().setK(clusteringK).setSeed(1L)
    val model = bkm.fit(featureDF)

    val predictDF: Dataset[Row] = model.transform(featureDF).select(Constants.GIS_JOIN, "features", "prediction")

    val evaluator = new ClusteringEvaluator()
    val silhouette = evaluator.evaluate(predictDF)
    Logger.log(s"BisectingKMeans: Silhouette (collection = '$collectionName', k = $clusteringK, #PCs = $noOfPCs): $silhouette")
  }
}
