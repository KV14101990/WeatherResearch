package com.app.utilities

import org.apache.spark.sql.{ SparkSession}

object CommonUtils extends SparkComputation {

  def readData(spark : SparkSession,path : String) = {
    spark.sparkContext.textFile(path)
  }
}
