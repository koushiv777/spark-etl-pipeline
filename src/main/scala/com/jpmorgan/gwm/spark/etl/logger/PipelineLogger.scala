package com.jpmorgan.gwm.spark.etl.logger

import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by koushiv777 on 5/21/17.
  */
trait PipelineLogger {
  @transient protected lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)
}
