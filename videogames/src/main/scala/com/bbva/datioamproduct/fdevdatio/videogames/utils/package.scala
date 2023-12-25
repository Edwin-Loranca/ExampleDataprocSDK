package com.bbva.datioamproduct.fdevdatio.videogames

import com.bbva.datioamproduct.fdevdatio.videogames.common.ColumnConstants.{PlatformCol, ReleaseYearCol}
import com.bbva.datioamproduct.fdevdatio.videogames.common.ConfigConstants._
import com.typesafe.config.Config
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Column, Dataset, Row, functions => f}

import scala.collection.convert.ImplicitConversions.`set asScala`

package object utils {

  def difference(l1: Seq[String], l2: Seq[String]): Seq[Column] =
    l1.diff(l2).map(colName => f.col(colName))

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
    def plataformaMenosVendida: Dataset[Row] = {
      // Get the platform with the least sold games
      val least_sales_platform: String = dataSet
        .select(f.col(PlatformCol), f.col("global_sales_per"))
        .groupBy(f.col(PlatformCol)).agg(f.sum("global_sales_per").alias("sum_sales_platform"))
        .sort(f.col("sum_sales_platform").asc)
        .collect()(0)
        .getAs[String](PlatformCol)

      dataSet.filter(f.col(PlatformCol) === least_sales_platform)
    }

    //Punto 1.3
    // Realiza una función que devuelva el top 3 de los juegos más vendidos por cada año.
    def top3MasVendidosXAnio: Dataset[Row] = {

//      var ds = dataSet.select(
//        f.col("global_sales_per"),
//        f.col("videogame_name"),
//        f.year(f.to_timestamp(f.col("release_year"), "yyy-MM-dd")).alias("year")
//      )

//      val window_1 = Window.partitionBy(f.col("year")).orderBy(f.col("global_sales_per").desc)

//      ds = ds.withColumn("rango", f.row_number.over(window_1)).orderBy(f.col("year"),f.col("rango").asc)
//      ds = ds.filter(f.col("rango") <= 3)
//      ds

      val yearSalesWindow = Window.partitionBy(f.col("year")).orderBy(f.col("global_sales_per").desc)

      dataSet
        .select(
          f.col("*"),
          f.year(f.to_timestamp(f.col(ReleaseYearCol))).alias("year")
        )
        .select(
          f.col("*"),
          f.rank().over(yearSalesWindow).alias("rank")
        )
        .filter(f.col("rank").isin(1, 2, 3))
        .select(
          f.col("year"),
          f.col("rank"),
          f.col("videogame_name"),
          f.col("global_sales_per")
        )
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
    def top10MasVendidosXConsola: Dataset[Row] = {

      val allowedConsoles: Seq[String] = "3DS,GB,GBA,NES,N64,SNES,Wii,WiiU,XB,XONE,X360,PS,PS2,PS3,PS4,PSP,PSV".split(",")

      val platformWindow = Window.partitionBy(f.col(PlatformCol)).orderBy(f.col("global_sales_per").desc)

      dataSet
        .filter(f.col(PlatformCol).isin(allowedConsoles:_*))
        .select(
          f.col("*"),
          f.rank().over(platformWindow).alias("rank")
        )
        .filter(f.col("rank").isin((1 to 10).toList :_*))

    }

    //Punto 1.5
    /*
    - Añadir al dataframe una columna llamada “complete_name” resultante de la concatenación de las columnas “Publisher” y “Platform”
    - Añadir al dataframe una columna llamada “clasification” generada a partir de la siguiente regla:
        Si es de plataforma nintendo->E, Si el género es Shooter o Fighting -> M, Si el género es Role-Playing -> T, otherwiswe E
     */
    def asignacionClasificiacion: Dataset[Row] = {
      dataSet
        .select(
          f.col("*"),
          f.concat(f.col("publisher_name"), f.lit(" "), f.col(PlatformCol)).alias("complete_name"),
          f.when(f.col(PlatformCol) === "nintendo", "E")
            .when((f.col("videgoame_genre") === "Shooter") || (f.col("videgoame_genre") === "Fighting"), "M")
            .when(f.col("videgoame_genre") === "Role-Playing", "T")
            .otherwise("E")
            .alias("classification")
        )
    }

    // Punto 1.6
    // Realiza una función que una los dataframes obtenidos en los pasos 1.4 y 1.5.
  }
}
