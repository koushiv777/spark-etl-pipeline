package com.jpmorgan.gwm.spark.etl.transformation.source

import com.jpmorgan.gwm.spark.etl.constants.PipelineConstants
import com.jpmorgan.gwm.spark.etl.context.PipelineEngineContext
import com.jpmorgan.gwm.spark.etl.utils.{FileUtils, HDFSUtils}
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame

/**
  * Created by koushiv777 on 5/28/17.
  */
class SQLSourceTransformation(config: Config, context: PipelineEngineContext) extends SourceTransformation(config, context) {
  override def read(): Unit = {
    logger.info(s"Started reading from input $name")
    setJobDescription()

    val fileType: String = config getString PipelineConstants.PATH_SQL_FILE_TYPE
    logger.info(s"Hive Source Transformation fileType $fileType")
    val sql = fileType match {
      case "hdfs" =>
        HDFSUtils() getFileContent config.getString(PipelineConstants.PATH_SQL_FILE_LOCATION)
      case "local" =>
        FileUtils getFileContent config.getString(PipelineConstants.PATH_SQL_FILE_LOCATION)
      case _ =>
        config getString PipelineConstants.PATH_SQL_LITERAL
    }

    logger.info(s"Executing Spark SQL $sql")
    data = context.sparkSession.sql(sql)

    isFinished = true
  }

  override var data: DataFrame = _
  override var isFinished: Boolean = _
}
