package com.app.weather

import com.app.compute.{PressureComputation,TemperatureComputation}
import com.app.utilities.SparkComputation
import org.apache.log4j.LogManager

/**
 * This is the main class for Weather Research
 * Data Analysis for Weather forecast
 */
object WeatherResearchMain extends SparkComputation{
  val logger = LogManager.getLogger(this.getClass.getName)

  def main(args : Array[String]) {

    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val pressure = new PressureComputation()
    // computing pressure data and getting the data
    logger.info("pressure computation")
    val pressureDF = pressure.pressureCompute(spark)

    // computing pressure data and getting the data
    val temperature = new TemperatureComputation()
    logger.info("temperature computation")
    val temperatureDF = temperature.temperatureCompute(spark)

    // analysing data
    val WeatherDataAnalysis  = new WeatherDataAnalysis()
    WeatherDataAnalysis.pressureDataAnalysis(spark,pressureDF)
    WeatherDataAnalysis.temperatureDataAnalysis(spark,temperatureDF)

    //stop the spark session
    def stopSpark(): Unit = stopSpark()

  }
}
