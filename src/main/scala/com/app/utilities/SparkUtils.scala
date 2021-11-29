package com.app.utilities

import org.apache.spark.sql.SparkSession

trait SparkComputation {

  // defining SparkSession
  lazy val spark =
    SparkSession
      .builder()
      .master("local[*]")
      .appName("WeatherResearch")
      .enableHiveSupport()
      .getOrCreate()

  def stopSpark(): Unit = spark.stop()
}