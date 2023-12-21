package com.bbva.datioamproduct.fdevdatio.videogames

import com.bbva.datioamproduct.fdevdatio.videogames.common.ConfigConstants._
import com.typesafe.config.Config
import org.apache.spark.sql.{Dataset, Row}

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

//  implicit class

}
