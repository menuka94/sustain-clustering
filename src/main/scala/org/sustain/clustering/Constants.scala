package org.sustain.clustering

object Constants {
  val DB_HOST: String = sys.env("DB_HOST")
  val DB_PORT: Int = sys.env("DB_PORT").toInt
  val SPARK_MASTER: String = sys.env("SPARK_MASTER")
  val SPARK_EXECUTOR_CORES: Int = sys.env("SPARK_EXECUTOR_CORES").toInt
  val SPARK_INITIAL_EXECUTORS: Int = sys.env("SPARK_INITIAL_EXECUTORS").toInt
  val SPARK_MIN_EXECUTORS: Int = sys.env("SPARK_MIN_EXECUTORS").toInt
  val SPARK_MAX_EXECUTORS: Int = sys.env("SPARK_MAX_EXECUTORS").toInt
  val SPARK_EXECUTOR_MEMORY: String = sys.env("SPARK_EXECUTOR_MEMORY")
  val SPARK_BACKLOG_TIMEOUT: String = sys.env("SPARK_BACKLOG_TIMEOUT")
  val SPARK_IDLE_TIMEOUT: String = sys.env("SPARK_IDLE_TIMEOUT")

  val GIS_JOIN = "gis_join"
}
