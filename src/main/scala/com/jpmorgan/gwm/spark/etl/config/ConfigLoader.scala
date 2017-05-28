package com.jpmorgan.gwm.spark.etl.config

import com.jpmorgan.gwm.spark.etl.constants.PipelineConstants
import com.jpmorgan.gwm.spark.etl.exception.PipelineConfigurationException
import com.jpmorgan.gwm.spark.etl.logger.PipelineLogger
import com.typesafe.config.{Config, ConfigFactory}

import scala.reflect.io.File

/**
  * Created by koushiv777 on 5/21/17.
  */
class ConfigLoader(private val applicationConf: String, private val workFlowConf: String) extends PipelineLogger{
  private val profilesSys: String = System getProperty PipelineConstants.PIPELINE_PROFILE_PROPERTY

  private val applicationConfFile = File(applicationConf)

  if(!applicationConfFile.isFile || !applicationConfFile.exists){
    throw PipelineConfigurationException(s"Application Configuration $applicationConf is not a file or it's doesn't exists. Please validate")
  }
  logger.debug(s"Application Configuration found in ${applicationConfFile.path}")

  private val applicationConfig: Config = ConfigFactory.parseFile(applicationConfFile.jfile)

  private val workflowConfFile = File(workFlowConf)
  if(!workflowConfFile.isFile || !workflowConfFile.exists){
    throw PipelineConfigurationException(s"Workflow Configuration $workFlowConf is not a file or it's doesn't exists. Please validate")
  }

  private val workflowConfig: Config = ConfigFactory.parseFile(workflowConfFile.jfile)

  private val profileConfigs: Seq[Config] = List()
  if(profilesSys != null && !profilesSys.isEmpty){
    logger.debug(s"Spark Pipeline Engine Profiles $profilesSys")
    val applicationConfDir = applicationConfFile.path.substring(0, applicationConfFile.path.lastIndexOf(File.separator))
    logger.debug(s"Application Configuration directory $applicationConfDir")
    val profiles = profilesSys.split(",")
    val profileConfFiles: Array[File] = profiles.map(profile => File(applicationConfDir + File.separator + "application-" + profile + ".conf"))
    profileConfigs ++ profileConfFiles.map(confFile => ConfigFactory.parseFile(confFile.jfile))
  }

  private var tempConfig: Config = workflowConfig
  profileConfigs.foreach(config => {
    tempConfig = tempConfig.withFallback(config)
  })

  private val finalConfig: Config = tempConfig.withFallback(applicationConfig).resolve()

  def getPipelineConfig: Config = finalConfig
}

object ConfigLoader {
  def apply(applicationConf: String, workFlowConf: String): ConfigLoader = new ConfigLoader(applicationConf, workFlowConf)
}
