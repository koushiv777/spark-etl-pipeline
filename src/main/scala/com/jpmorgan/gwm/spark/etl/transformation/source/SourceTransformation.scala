package com.jpmorgan.gwm.spark.etl.transformation.source

import com.jpmorgan.gwm.spark.etl.constants.PipelineConstants
import com.jpmorgan.gwm.spark.etl.context.PipelineEngineContext
import com.jpmorgan.gwm.spark.etl.transformation.BaseTransformation
import com.typesafe.config.Config

/**
  * Created by koushiv777 on 5/28/17.
  */
trait SourceTransformation extends BaseTransformation {
  def read(): Unit
}

object SourceTransformation {
  def apply(config: Config, context: PipelineEngineContext): SourceTransformation = config.getString(PipelineConstants.PATH_SOURCE_TYPE) match {
    case "sql" => new SQLSourceTransformation(config, context)
    case "hive" => new HiveSourceTransformation(config, context)
  }
}
