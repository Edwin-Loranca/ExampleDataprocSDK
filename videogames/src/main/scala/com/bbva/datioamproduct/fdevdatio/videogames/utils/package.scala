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
    // Realiza una función que obtenga como resultado el promedio de ventas en Japón, el promedio de ventas en EUA y el promedio de ventas en todo el mundo.
    def promediosVenta: Dataset[Row] = {
      dataSet
        .select(
          f.mean(f.col("na_sales_per")).alias("Promedio_ventas_japon"),
          f.mean(f.col("jp_sales_per")).alias("Promedio_ventas_EUA"),
          f.mean(f.col("global_sales_per")).alias("Promedio_ventas_globales")
        )
    }

    // Punto 1.2
    // Realizar una función que obtenga un DataFrame que devuelva la información de la plataforma con menos ventas en todo el mundo
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
    //Punto 1.3
    // Realiza una función que devuelva el top 3 de los juegos más vendidos por cada año.
    def top3MasVendidos: Dataset[Row] = {

      var ds = dataSet.select(
        f.col("global_sales_per"),
        f.col("videogame_name"),
        f.year(f.to_timestamp(f.col("release_year"), "yyy-MM-dd")).alias("year")
      )

      val window_1 = Window.partitionBy(f.col("year")).orderBy(f.col("global_sales_per").desc)
      ds = ds.withColumn("rango", f.row_number.over(window_1)).orderBy(f.col("year"),f.col("rango").asc)
      ds = ds.filter(f.col("rango") <= 3)
      ds
    }

    // Punto 1.4
    /*
    Realiza una función que devuelva el top 10 de los juegos más vendidos por cada consola
    Ten en cuenta lo siguiente:
      Nintendo (3DS, GB, GBA, NES, N64, SNES, Wii, WiiU)
      Xbox (XB, XONE, X360)
      Play Station (PS, PS2, PS3, PS4, PSP, PSV)
    NOTA: Aplicar un filtro de los juegos que no pertenecen a estas consolas.
     */

    //Punto 1.5
    /*
    - Añadir al dataframe una columna llamada “complete_name” resultante de la concatenación de las columnas “Publisher” y “Platform”
    - Añadir al dataframe una columna llamada “clasification” generada a partir de la siguiente regla:
        Si es de plataforma nintendo->E, Si el género es Shooter o Fighting -> M, Si el género es Role-Playing -> T, otherwiswe E
     */
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

    // Punto 1.6
    // Realiza una función que una los dataframes obtenidos en los pasos 1.4 y 1.5.
  }
}
