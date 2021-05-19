package org.sustain.clustering

import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object KMeansClustering {
  def runClustering(spark: SparkSession,
                    inputCollection: Dataset[Row],
                    clusteringFeatures: Array[String],
                    clusteringK: Int,
                    noOfPCs: Int,
                    collectionName: String): Unit = {
    import spark.implicits._

    var clusteringCollection = inputCollection

    val assembler: VectorAssembler = new VectorAssembler()
      .setInputCols(clusteringFeatures)
      .setOutputCol("features")

    val featureDF: Dataset[Row] = assembler.transform(clusteringCollection).select(Constants.GIS_JOIN, "features")

    val kMeans = new KMeans().setK(clusteringK).setSeed(1L)
    val kMeansModel: KMeansModel = kMeans.fit(featureDF)

    val predictDF: Dataset[Row] = kMeansModel.transform(featureDF).select(Constants.GIS_JOIN, "features", "prediction")

    val evaluator = new ClusteringEvaluator()
    val silhouette = evaluator.evaluate(predictDF)
    SustainClustering.log(s"KMeans: Silhouette (collection = '$collectionName', k = $clusteringK, #PCs = $noOfPCs): $silhouette")
  }

}