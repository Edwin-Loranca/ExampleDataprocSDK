package com.bbva.datioamproduct.fdevdatio.videogames

import com.bbva.datioamproduct.fdevdatio.videogames.common.ConfigConstants._
import com.typesafe.config.Config
import org.apache.spark.sql.{Dataset, Row, functions => f, types => t}
import org.apache.spark.sql.expressions.Window
import scala.:+
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
    //Punto 1.3
    def top3MasVendidos: Dataset[Row] = {

      var ds= dataSet.select(
        f.col("global_sales_per"),
        f.col("videogame_name"),
        f.year(f.to_timestamp(f.col("release_year"), "yyy-MM-dd")).alias("year")
      )


      val window_1 = Window.partitionBy(f.col("year")).orderBy(f.col("global_sales_per").desc)
      ds = ds.withColumn("rango", f.row_number.over(window_1)).orderBy(f.col("year"),f.col("rango").asc)
      ds = ds.filter(f.col("rango") <= 3)
      ds

    }
  }

}
