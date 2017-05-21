package com.jpmorgan.gwm.spark.etl

import com.jpmorgan.gwm.spark.etl.config.ConfigLoader
import com.jpmorgan.gwm.spark.etl.context.PipelineEngineContext
import com.jpmorgan.gwm.spark.etl.logger.PipelineLogger

/**
  * Created by koushiv777 on 5/21/17.
  */
object SparkETLPipelineEngineApp extends PipelineLogger{

  def main(args: Array[String]): Unit = {
    logger.info("Starting Spark ETL Pipeline Engine")
    logger.info(s"Arguments passed for Pipeline Engine ${args.toList}")

    if(args.length < 2) {
      throw new IllegalArgumentException("Minimum 2 configuration files needed application.conf and workflow.conf for workflow definition")
    }

    if(args(1).indexOf("application") != -1){
      throw new IllegalArgumentException("First Argument need to be path of application.conf")
    }

    val config = ConfigLoader(args(0), args(1)).getPipelineConfig

    logger.debug(s"Configuration loaded for Spark Pipeline Configuration $config")

    val context = PipelineEngineContext(config)

    logger.info(s"Spark Context Created")

  }

}
