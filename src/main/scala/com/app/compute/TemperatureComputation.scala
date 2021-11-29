package com.app.compute

import com.app.constants.Constants
import com.app.schema.DataSchema
import com.app.utilities.CommonUtils.readData
import com.app.utilities.SparkComputation
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.lit

/**
 * this computation
 * - picks temperature observations data from HDFS
 * - cleans and validates data
 * - fill NAN values in new columns
 * - returns final temperature data by union of all dataframes
 * - Write the result in hive table in parquet format.
 */
class TemperatureComputation extends SparkComputation {

  val logger = LogManager.getLogger(this.getClass.getName)

  /**
   * This method reads temperature data and returns a final dataframe
   * @param sparkSession
   * @return
   */
  def temperatureCompute(sparkSession: SparkSession): DataFrame = {
    var temperatureDF = spark.emptyDataFrame
    val temperature_Schema = new DataSchema
    try {

      //Reading temperature data
      val rawManualTempData = readData(spark,Constants.Temperature_Manual_Station)
      val rawAutomaticTempData = readData(spark,Constants.Temperature_Automatic_Station)
      val rawTempData1961 = readData(spark,Constants.Temperature_1961)
      val rawTempData1756 = readData(spark,Constants.Temperature_1756)
      val rawTempData1859 = readData(spark,Constants.Temperature_1859)

      // creating RDDs, dataframes, dropping extra columns and adding required columns in data
      val ManualTempDataRDD = rawManualTempData.map(x => x.split("\\s+")).map(x=>temperature_Schema.TempSchema_1961_Manual_Auto(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))
      val manualTempDataDF = spark.createDataFrame(ManualTempDataRDD)
      val manualStationTempDF = manualTempDataDF
        .withColumn("station", lit("manual"))
      val manualStationFinalDF = manualStationTempDF.select("year","month","day","morning","noon","evening","temperature_min","temperature_max","estimatedDiurnalMean","station")

      val automaticTempDataRDD = rawAutomaticTempData.map(x => x.split("\\s+")).map(x=>temperature_Schema.TempSchema_1961_Manual_Auto(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))
      val automaticTempDataDF = spark.createDataFrame(automaticTempDataRDD)
      val automaticStationTempDF = automaticTempDataDF
        .withColumn("station", lit("automatic"))
      val automaticStationFinalDF = automaticStationTempDF.select("year","month","day","morning","noon","evening","temperature_min","temperature_max","estimatedDiurnalMean","station")

      val tempData1961RDD = rawTempData1961.map(x => x.split("\\s+")).map(x=>temperature_Schema.TempSchema_1961_Manual_Auto(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))
      val tempData1961DF = spark.createDataFrame(tempData1961RDD)
      val tempData1961transientDF = tempData1961DF.withColumn("station", lit("NAN"))
      val tempData1961FinalDF = tempData1961transientDF.select("year","month","day","morning","noon","evening","temperature_min","temperature_max","estimatedDiurnalMean","station")

      val tempData1756RDD = rawTempData1756.map(x => x.split("\\s+")).map(x=>temperature_Schema.UncleanTemperatureSchema(x(0),x(1),x(2),x(3),x(4),x(5),x(6)))
      val tempData1756DF = spark.createDataFrame(tempData1756RDD)
        .drop("col1")
      val tempData1756transientDF = tempData1756DF
        .withColumn("temperature_min", lit("NaN"))
        .withColumn("temperature_max", lit("NaN"))
        .withColumn("estimatedDiurnalMean", lit("NaN"))
        .withColumn("station", lit("NaN"))
      val tempData1756FinalDF = tempData1756transientDF.select("year","month","day","morning","noon","evening","temperature_min","temperature_max","estimatedDiurnalMean","station")

      val tempData1859RDD = rawTempData1859.map(x => x.split("\\s+")).map(x=>temperature_Schema.TemperatureSchema_1859(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7)))
      val tempData1859DF = spark.createDataFrame(tempData1859RDD)
      val tempData1859transientDF = tempData1859DF
        .withColumn("station", lit("NAN"))
        .withColumn("estimatedDiurnalMean",lit("NAN"))
      val tempData1859FinalDF = tempData1859transientDF.select("year","month","day","morning","noon","evening","temperature_min","temperature_max","estimatedDiurnalMean","station")

      // union of temperature data
      temperatureDF = manualStationFinalDF
        .union(automaticStationFinalDF)
        .union(tempData1961FinalDF)
        .union(tempData1756FinalDF)
        .union(tempData1859FinalDF)
        .dropDuplicates()

      // hive table creation to store final temperature data
      spark.sql("""CREATE TABLE TemperatureData(
              year String,
              month String,
              day String,
              morning String,
              noon String,
              evening String,
              minimum String,
              maximum String,
              estimated_diurnal_mean String,
              station String)
            STORED AS PARQUET""")

      // Writing data to hive table
      temperatureDF.write.mode(SaveMode.Overwrite).saveAsTable("TemperatureDataTable")
    }
    catch
    {
      case ex: Exception =>
        logger.error("Temperature data could not be computed",ex)
    }
    temperatureDF
  }

}