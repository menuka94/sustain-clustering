package org.sustain.clustering

import org.apache.spark.sql.SparkSession
import org.sustain.clustering.SustainClustering.log

object SparkManager {

  def logEnv(): Unit = {
    log(">>> Log Environment")
    log("USE_KUBERNETES: " + Constants.USE_KUBERNETES)
    if (Constants.USE_KUBERNETES) {
      log("SPARK_MASTER: " + Constants.KUBERNETES_SPARK_MASTER)
    } else {
      log("SPARK_MASTER: " + Constants.SPARK_MASTER)
    }
    log("DB_HOST: " + Constants.DB_HOST)
    log("DB_PORT: " + Constants.DB_PORT)
  }

  def getSparkSession (collection1: String): SparkSession = {
    if (Constants.USE_KUBERNETES) {
      SparkSession.builder()
        .master(Constants.KUBERNETES_SPARK_MASTER)
        .appName(s"Clustering ('$collection1'): Varying #clusters")
        .config("spark.submit.deployMode", "cluster")
        .config("spark.mongodb.input.uri",
          "mongodb://" + Constants.DB_HOST + ":" + Constants.DB_PORT + "/sustaindb." + collection1)
        .config("spark.kubernetes.container.image", Constants.SPARK_DOCKER_IMAGE)
//        .config("spark.dynamicAllocation.enabled", "false")
//        .config("spark.dynamicAllocation.shuffleTracking.enabled", "false")
        .config("spark.kubernetes.namespace", "spark-demo")
        .config("spark.kubernetes.pod.name", "sustain-clustering-pod")
        .config(" spark.kubernetes.authenticate.driver.serviceAccountName", "spark")
        .config("spark.executor.instances", 100)
//        .config("spark.dynamicAllocation.minExecutors", Constants.SPARK_INITIAL_EXECUTORS)
//        .config("spark.dynamicAllocation.maxExecutors", Constants.SPARK_MAX_EXECUTORS)
        .config("spark.executor.memory", Constants.SPARK_EXECUTOR_MEMORY)
        .getOrCreate()
    } else {
      SparkSession.builder()
        .master(Constants.SPARK_MASTER)
        .appName(s"Clustering ('$collection1'): Varying #clusters")
        .config("spark.submit.deployMode", "cluster")
        .config("spark.mongodb.input.uri",
          "mongodb://" + Constants.DB_HOST + ":" + Constants.DB_PORT + "/sustaindb." + collection1)
        .config("spark.dynamicAllocation.enabled", "true")
        .config("spark.dynamicAllocation.shuffleTracking.enabled", "true")
        .config("spark.dynamicAllocation.minExecutors", Constants.SPARK_INITIAL_EXECUTORS)
        .config("spark.dynamicAllocation.maxExecutors", Constants.SPARK_MAX_EXECUTORS)
        .config("spark.executor.memory", Constants.SPARK_EXECUTOR_MEMORY)
        .getOrCreate()
    }
  }
}
