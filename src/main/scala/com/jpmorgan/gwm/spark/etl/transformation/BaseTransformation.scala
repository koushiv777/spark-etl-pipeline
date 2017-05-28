package com.jpmorgan.gwm.spark.etl.transformation

import com.jpmorgan.gwm.spark.etl.context.PipelineEngineContext
import com.typesafe.config.Config

/**
  * Created by koushiv777 on 5/28/17.
  */
abstract class BaseTransformation(val config: Config, context: PipelineEngineContext) extends Transformation {
  override val name: String = config.entrySet().iterator().next().getKey

  def setJobDescription(): Unit =  context.sparkContext.setJobDescription(name)
}
