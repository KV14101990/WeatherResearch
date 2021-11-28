package com.app.utilities

import org.apache.spark.sql.SparkSession

trait SparkComputation {

  //Define a spark session
  lazy val spark =
    SparkSession
      .builder()
      .master("local[*]")
      .appName("WeatherResearchMain")
      .enableHiveSupport()
      .getOrCreate()

  def stopSpark(): Unit = spark.stop()
}