package com.bbva.datioamproduct.fdevdatio.videogames

import com.bbva.datioamproduct.fdevdatio.videogames.common.ConfigConstants._
import com.bbva.datioamproduct.fdevdatio.videogames.utils.{IOUtils, SuperConfig, extendDataset}
import com.datio.dataproc.sdk.api.SparkProcess
import com.datio.dataproc.sdk.api.context.RuntimeContext
import com.typesafe.config.Config
import org.apache.spark.sql.{Dataset, Row, functions => f}
import org.slf4j.{Logger, LoggerFactory}


class VideogamesJob extends SparkProcess with IOUtils{

  private val logger:Logger = LoggerFactory.getLogger(this.getClass)

  override def getProcessId: String = "VideogamesJob"

  override def runProcess(runtimeContext: RuntimeContext): Int = {

    val config: Config = runtimeContext.getConfig

    val mapDs: Map[String, Dataset[Row]] = config.readInputs

    // Punto 1.1
    mapDs("videogamesSales").promediosVenta.show

    //Punto 1.5
    val videogamesInfoDs: Dataset[Row] = mapDs("videogamesInfo")
    videogamesInfoDs.creacionColumnas.show()
    0
  }
}
