package com.jpmorgan.gwm.spark.etl.exception

/**
  * Created by koushiv777 on 5/21/17.
  */
case class PipelineConfigurationException(val message: String = "", cause:Throwable = None.orNull) extends RuntimeException(message, cause)
