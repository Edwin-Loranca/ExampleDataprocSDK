package com.bbva.datioamproduct.fdevdatio.videogames

import com.bbva.datioamproduct.fdevdatio.videogames.common.ConfigConstants._
import com.bbva.datioamproduct.fdevdatio.videogames.utils.{IOUtils, SuperConfig, extendDataset}
import com.datio.dataproc.sdk.api.SparkProcess
import com.datio.dataproc.sdk.api.context.RuntimeContext
import com.typesafe.config.Config
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Column, Dataset, Row, functions => f}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{functions => f}
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


    // Punto 1.1
    mapDs("videogamesSales").promediosVenta.show

    // Punto 1.2
    mapDs("videogamesInfo").leastSalesPlatformInfo(mapDs("videogamesSales")).show


    //Punto 1.4
    def topByConsole: Dataset[Row] = {
      val info = mapDs("videogamesInfo")
      val sales = mapDs("videogamesSales")

      val consoles = List("3DS", "GB", "GBA", "NES", "N64", "SNES", "Wii", "WiiU", "XB", "XONE", "X360", "PS", "PS2", "PS3", "PS4", "PSP", "PSV")
      val window = Window.partitionBy(f.col("platform_na")).orderBy(f.desc("global_sales_per"))
      info
        .join(sales, Seq("videogame_id"), "inner")
        .filter(f.col("platform_na").isin(consoles: _*))
        .select(
          f.col("platform_na"),
          f.col("videogame_name"),
          f.rank().over(window).alias("rank")
        )
        .filter("rank <= 10")
    }

    val topGamesByConsole: Dataset[Row] = topByConsole
    topGamesByConsole.show()
    //Punto 1.5
    val videogamesInfoDs: Dataset[Row] = mapDs("videogamesInfo")
    videogamesInfoDs.creacionColumnas.show()

    def concatDf(dataSet1: Dataset[Row], dataSet2: Dataset[Row]): Dataset[Row] = {
      dataSet1
        .join(dataSet2, Seq("videogame_name"), "inner")
        .select("*")
    }

    val dsFinal: Dataset[Row] = concatDf(topGamesByConsole, videogamesInfoDs)
    dsFinal.show()
    0
  }
}
