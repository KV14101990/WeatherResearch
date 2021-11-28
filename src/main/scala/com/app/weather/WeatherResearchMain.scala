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
    val pressureDF = pressure.pressureCompute(spark)
    //todo
    //    val temperature = new TemperatureComputation()
    //   temperature.temperatureCompute(spark)

    val WeatherDataAnalysis  = new WeatherDataAnalysis()
    WeatherDataAnalysis.dataAnalysis(spark,pressureDF)

    //stop the spark session
    def stopSpark(): Unit = stopSpark()

  }

}
