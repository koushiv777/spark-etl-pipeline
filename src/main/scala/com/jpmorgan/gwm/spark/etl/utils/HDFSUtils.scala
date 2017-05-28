package com.jpmorgan.gwm.spark.etl.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.io.Source

/**
  * Created by koushiv777 on 5/28/17.
  */
class HDFSUtils(val config: Configuration) {
  def getFileContent(path: String): String = {
    val fs = FileSystem.get(config);
    Source.fromInputStream(fs.open(new Path(path)), "UTF-8").mkString
  }
}

object HDFSUtils {
  def apply(config: Configuration = new Configuration()) = new HDFSUtils(config)
}
