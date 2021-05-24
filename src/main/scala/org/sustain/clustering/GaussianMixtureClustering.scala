package org.sustain.clustering

import org.apache.spark.ml.clustering.{GaussianMixture, KMeans, KMeansModel}
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object GaussianMixtureClustering {
  def runClustering(spark: SparkSession,
                    inputCollection: Dataset[Row],
                    clusteringFeatures: Array[String],
                    k: Int,
                    collectionName: String): Unit = {
    import spark.implicits._

    var clusteringCollection = inputCollection

    val assembler: VectorAssembler = new VectorAssembler()
      .setInputCols(clusteringFeatures)
      .setOutputCol("features")

    val featureDF: Dataset[Row] = assembler.transform(clusteringCollection).select(Constants.GIS_JOIN, "features")

    val gmm = new GaussianMixture().setK(k).setSeed(1L)
    val model = gmm.fit(featureDF)

    val predictDF: Dataset[Row] = model.transform(featureDF).select(Constants.GIS_JOIN, "features", "prediction")

    val evaluator = new ClusteringEvaluator()
    val silhouette = evaluator.evaluate(predictDF)
    SustainClustering.log(s"GaussianMixture: Silhouette (collection = '$collectionName', k = $k): $silhouette")
  }
}
