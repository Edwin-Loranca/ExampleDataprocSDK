package com.bbva.datioamproduct.fdevdatio.videogames

import com.bbva.datioamproduct.fdevdatio.videogames.common.ConfigConstants._
import com.bbva.datioamproduct.fdevdatio.videogames.utils.{IOUtils, SuperConfig}
import com.datio.dataproc.sdk.api.SparkProcess
import com.datio.dataproc.sdk.api.context.RuntimeContext
import com.typesafe.config.Config
import org.apache.spark.sql.{Dataset, Row}
import org.slf4j.{Logger, LoggerFactory}


class VideogamesJob extends SparkProcess with IOUtils{

  private val logger:Logger = LoggerFactory.getLogger(this.getClass)

  override def getProcessId: String = "VideogamesJob"

  override def runProcess(runtimeContext: RuntimeContext): Int = {

    val config: Config = runtimeContext.getConfig

    val mapDs: Map[String, Dataset[Row]] = config.readInputs

    mapDs("videogamesInfo").show()
    mapDs("videogamesSales").show()
    0
  }
}
