package com.app.weather

import com.app.utilities.SparkComputation
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 *todo
 */
class WeatherDataAnalysis extends SparkComputation{
  val logger = LogManager.getLogger(this.getClass.getName)

  /**
   * This returns insights for Pressure Data
   * @param sparkSession
   * @param pressureDF
   */
  def pressureDataAnalysis(sparkSession: SparkSession,pressureDF:DataFrame):Unit = {
    val pressureDFOutputCount =  pressureDF.count()
    logger.info("Transformed pressure output data count is " + pressureDFOutputCount)

  }

  /**
   * This returns insights for Temperature Data
   * @param sparkSession
   * @param pressureDF
   */
  def temperatureDataAnalysis(sparkSession: SparkSession,temperatureDF:DataFrame):Unit = {
    val temperatureDFOutputCount =  temperatureDF.count()
    logger.info("Transformed temperature output data count is " + temperatureDFOutputCount)

  }
}
