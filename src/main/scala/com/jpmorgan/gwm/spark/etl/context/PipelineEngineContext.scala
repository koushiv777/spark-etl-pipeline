package com.jpmorgan.gwm.spark.etl.context

import com.jpmorgan.gwm.spark.etl.constants.PipelineConstants._
import com.typesafe.config.Config
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.SparkSession._

/**
  * Created by koushiv777 on 5/22/17.
  */
class PipelineEngineContext(val config:Config) {
  val sparkConf: SparkConf = getSparkConfiguration()
  val sparkSession: SparkSession = builder().config(sparkConf).getOrCreate()
  val sparkContext: SparkContext = sparkSession.sparkContext
  val sqlContext: SQLContext = sparkSession.sqlContext

  def getSparkConfiguration(): SparkConf ={
    val conf = new SparkConf()
    if(config.hasPath(PATH_APPLICATION_NAME)) conf.setAppName(config getString PATH_APPLICATION_NAME)

    if(config.hasPath(PATH_SPARK_CONF_MASTER)) conf setMaster config.getString(PATH_SPARK_CONF_MASTER)

    if(config.hasPath(PATH_SPARK_CONF_EXECUTORS)) conf.set("spark.executor.instances", config getString PATH_SPARK_CONF_EXECUTORS)

    if(config.hasPath(PATH_SPARK_CONF_EXECUTOR_CORES)) conf.set("spark.executor.cores", config getString PATH_SPARK_CONF_EXECUTOR_CORES)

    if(config.hasPath(PATH_SPARK_CONF_EXECUTOR_MEMORY)) conf.set("spark.executor.memory", config getString PATH_SPARK_CONF_EXECUTOR_MEMORY)

    if(config.hasPath(PATH_SPARK_CONF_EXECUTORS) && config.hasPath(PATH_SPARK_CONF_EXECUTOR_CORES)){
      val executors: Int = config.getInt(PATH_SPARK_CONF_EXECUTORS)
      val cores: Int = config.getInt(PATH_SPARK_CONF_EXECUTOR_CORES)
      val shufflePartitions: Int = executors * cores
      conf.set("spark.sql.shuffle.partitions", shufflePartitions.toString)
    }
    conf
  }

}

object PipelineEngineContext {
  def apply(config: Config): PipelineEngineContext = new PipelineEngineContext(config)
}
