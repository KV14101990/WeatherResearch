package com.app.weather

import com.app.utilities.SparkComputation
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col

/**
 * - This class provides the data analysis pf pressure and temperature data
 * - logs info about data such as total count
 */
class WeatherDataAnalysis extends SparkComputation{
  val logger = LogManager.getLogger(this.getClass.getName)

  /**
   * Returns insights for Pressure Data
   * @param sparkSession
   * @param pressureDF
   */
  def pressureDataAnalysis(sparkSession: SparkSession,pressureDF:DataFrame):Unit = {
    val pressureDFOutputCount =  pressureDF.count()
    logger.info("Transformed pressure output data count is " + pressureDFOutputCount)
  }

  /**
   * Returns insights for Temperature Data
   * @param sparkSession
   * @param pressureDF
   */
  def temperatureDataAnalysis(sparkSession: SparkSession,temperatureDF:DataFrame):Unit = {
    val temperatureDFOutputCount =  temperatureDF.count()
    logger.info("Transformed temperature output data count is " + temperatureDFOutputCount)

  }
}
