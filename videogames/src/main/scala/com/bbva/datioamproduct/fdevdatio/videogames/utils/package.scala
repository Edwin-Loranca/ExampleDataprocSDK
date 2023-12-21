package com.bbva.datioamproduct.fdevdatio.videogames

import com.bbva.datioamproduct.fdevdatio.videogames.common.ConfigConstants._
import com.typesafe.config.Config
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.{functions => f}

import scala.collection.convert.ImplicitConversions.`set asScala`

package object utils {

  implicit class SuperConfig(config: Config) extends IOUtils {

    def readInputs: Map[String, Dataset[Row]] = {
      config.getObject(InputTag).keySet()
        .map(key => {

          val inputConfig: Config = config.getConfig(s"$InputTag.$key")
          val ds: Dataset[Row] = read(inputConfig)
          key -> ds
        }).toMap
    }

  }

  implicit class extendDataset(dataSet: Dataset[Row]) {

    // Punto 1.1
    def promediosVenta: Dataset[Row] = {
      dataSet
        .select(
          f.mean(f.col("na_sales_per")).alias("Promedio_ventas_japon"),
          f.mean(f.col("jp_sales_per")).alias("Promedio_ventas_EUA"),
          f.mean(f.col("global_sales_per")).alias("Promedio_ventas_globales")
          )
    }
  }

}
