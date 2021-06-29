package org.sustain.clustering

import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.sustain.Constants
import org.sustain.util.Logger

object KMeansClustering {
  def runClustering(inputCollection: Dataset[Row],
                    k: Int,
                    clusteringFeatures: Array[String],
                    collectionName: String): Unit = {
    val clusteringCollection = inputCollection

    val assembler: VectorAssembler = new VectorAssembler()
      .setInputCols(clusteringFeatures)
      .setOutputCol("assembled_features")

    var featureDF: Dataset[Row] = assembler.transform(clusteringCollection).select(Constants.GIS_JOIN, "assembled_features")
    featureDF = featureDF.drop("features").withColumnRenamed("assembled_features", "features")
    val kMeans = new KMeans().setK(k).setSeed(1L)
    val kMeansModel: KMeansModel = kMeans.fit(featureDF)

    val predictDF: Dataset[Row] = kMeansModel.transform(featureDF).select(Constants.GIS_JOIN, "features", "prediction")
    val evaluator = new ClusteringEvaluator()
    val silhouette = evaluator.evaluate(predictDF)
    Logger.log(s"KMeans (collection = '$collectionName', #clusters = $k): Silhouette = $silhouette")
  }
}
