package com.jpmorgan.gwm.spark.etl.transformation.source

import com.jpmorgan.gwm.spark.etl.constants.PipelineConstants
import com.jpmorgan.gwm.spark.etl.context.PipelineEngineContext
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame

/**
  * Created by koushiv777 on 5/28/17.
  */
class HiveSourceTransformation (config: Config, context: PipelineEngineContext) extends SourceTransformation(config, context) {
  override def read(): Unit = {
    logger.info(s"Started reading from input $name")
    setJobDescription()
    val table = config.getString(PipelineConstants.PATH_SOURCE_TABLE)
    logger.info(s"Reading HIV Table $table")
    data = context.sqlContext.read.table(table)
    isFinished = true
  }

  override var data: DataFrame = _
  override var isFinished: Boolean = _
}
