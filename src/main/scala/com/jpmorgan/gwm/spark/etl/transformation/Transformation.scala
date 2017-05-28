package com.jpmorgan.gwm.spark.etl.transformation

import com.jpmorgan.gwm.spark.etl.logger.PipelineLogger
import org.apache.spark.sql.DataFrame

/**
  * Created by koushiv777 on 5/28/17.
  */
trait Transformation extends PipelineLogger {
  var data: DataFrame
  var isFinished: Boolean
  val name:String
}
