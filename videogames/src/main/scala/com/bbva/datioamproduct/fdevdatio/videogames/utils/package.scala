package com.bbva.datioamproduct.fdevdatio.videogames

import com.bbva.datioamproduct.fdevdatio.videogames.common.ConfigConstants._
import com.typesafe.config.Config
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Column, Dataset, Row, functions => f}

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

    // Punto 1.2
    def leastSalesPlatformInfo(dataSet2: Dataset[Row]): Dataset[Row] = {
      def difference(l1: Seq[String], l2: Seq[String]): Seq[Column] =
        l1.diff(l2).map(colName => f.col(colName))

      val aux = dataSet
        .select(difference(dataSet.columns, Seq("cutoff_date")): _*)
        .join(dataSet2, Seq("videogame_id"), "inner")

      // Get the platform with the least sold games
      val windowPlatform = Window.partitionBy("platform_na")
      val least_sales_platform: String = aux
        .select(
          difference(aux.columns, Seq()) :+
          f.sum(f.col("global_sales_per")).over(windowPlatform).alias("sum_sales_platform"): _*
        )
        .sort(f.col("sum_sales_platform").asc)
        .collect()(0)
        .getAs[String]("platform_na")

      aux.filter(f.col("platform_na") === least_sales_platform)
    }


    //Punto 1.5
    def creacionColumnas: Dataset[Row] = {
      dataSet
        .select(
          //f.col("*"),
          f.concat(f.col("publisher_name"), f.lit(" "), f.col("platform_na")).alias("complete_name"),
          f.when(f.col("platform_na") === "nintendo", "E")
            .when((f.col("videgoame_genre") === "Shooter") || (f.col("videgoame_genre") === "Fighting"), "M")
            .when(f.col("videgoame_genre") === "Role-Playing", "T")
            .otherwise("E")
            .alias("clasification")
        )
    }

  }

}
