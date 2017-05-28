package com.jpmorgan.gwm.spark.etl.utils

import scala.io.Source

/**
  * Created by koushiv777 on 5/28/17.
  */
object FileUtils {
  def getFileContent(path: String): String = {
    Source.fromFile(path).mkString
  }
}
