package com.app.compute

import com.app.constants.Constants
import com.app.schema.DataSchema
import com.app.utilities.CommonUtils.readData
import com.app.utilities._
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.lit

/**
 * this computation
 * - picks pressure observations data from HDFS
 * - cleans and validates data
 * - fill NAN values in new columns
 * - returns final pressure data by union of all dataframes
 * - Write the result in hive table in parquet format.
 */
class PressureComputation extends SparkComputation{

  val logger = LogManager.getLogger(this.getClass.getName)

  /**
   * This method reads pressure data and returns a final dataframe
   * @param sparkSession
   * @return
   */
  def pressureCompute(sparkSession: SparkSession): DataFrame = {
    var pressureDF = spark.emptyDataFrame
    val pressure_Schema = new DataSchema
    try{
      //Reading Pressure Data
      val rawPressureData1756 = readData(spark,Constants.Pressure_1756)
      val rawPressureData1859 = readData(spark,Constants.Pressure_1859)
      val rawPressureData1862 = readData(spark,Constants.Pressure_1862)
      val rawPressureData1938 = readData(spark,Constants.Pressure_1938)
      val rawPressureData1961 = readData(spark,Constants.Pressure_1961)
      val rawManualPressureData = readData(spark,Constants.Pressure_Manual_Station)
      val rawAutoPressureData = readData(spark,Constants.Pressure_Automatic_Station)

      // creating RDD, dataframes, dropping extra columns and adding required columns in data
      val pressureData1756RDD = rawPressureData1756.map(x => x.split("\\s+")).map(x=>pressure_Schema.PressureSchema_1756(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))
      val pressureData1756DF = spark.createDataFrame(pressureData1756RDD)
      val pressureData1756tempDF = pressureData1756DF
        .withColumn("station", lit("NaN"))
        .withColumn("pressure_unit", lit("Swedish inches (29.69 mm)"))
        .withColumn("thermometer_observation_1", lit("NaN"))
        .withColumn("thermometer_observation_2", lit("NaN"))
        .withColumn("thermometer_observation_3", lit("NaN"))
        .withColumn("air_pressure_degC_1", lit("NaN"))
        .withColumn("air_pressure_degC_2", lit("NaN"))
        .withColumn("air_pressure_degC_3", lit("NaN"))

      val pressureData1756finalDF = pressureData1756tempDF.select(
        "year", "month", "day", "pressure_morning", "pressure_noon", "pressure_evening","station",
        "pressure_unit", "barometer_temp_1", "barometer_temp_2",
        "barometer_temp_3", "thermometer_observation_1",
        "thermometer_observation_2", "thermometer_observation_3",
        "air_pressure_degC_1", "air_pressure_degC_2", "air_pressure_degC_3")

      val pressureData1859RDD = rawPressureData1859.map(x => x.split("\\s+")).map(x=>pressure_Schema.PressureSchema_1859(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10), x(11)))
      val pressureData1859DF = spark.createDataFrame(pressureData1859RDD)
      val pressureData1859TempDF = pressureData1859DF
        .withColumn("station", lit("NaN"))
        .withColumn("pressure_unit", lit("0.1*Swedish inches (2.969 mm)"))
        .withColumn("barometer_temp_1", lit("NaN"))
        .withColumn("barometer_temp_2", lit("NaN"))
        .withColumn("barometer_temp_3", lit("NaN"))

      val pressureData1859FinalDF = pressureData1859TempDF.select(
        "year", "month", "day", "pressure_morning", "pressure_noon", "pressure_evening", "station", "pressure_unit",
        "barometer_temp_1", "barometer_temp_2",
        "barometer_temp_3", "thermometer_observation_1",
        "thermometer_observation_2", "thermometer_observation_3",
        "air_pressure_degC_1", "air_pressure_degC_2", "air_pressure_degC_3")

      val pressureData1862RDD = rawPressureData1862.map(x => x.split("\\s+")).map(x=>pressure_Schema.UncleanPressureSchema(x(0), x(1), x(2), x(3), x(4), x(5), x(6)))
      val pressureData1862DF = sparkSession.createDataFrame(pressureData1862RDD).drop("col1")
      val pressureData1862TempDF = pressureData1862DF
        .withColumn("station", lit("NaN"))
        .withColumn("pressure_unit", lit("mhg"))
        .withColumn("barometer_temp_1", lit("NaN"))
        .withColumn("barometer_temp_2", lit("NaN"))
        .withColumn("barometer_temp_3", lit("NaN"))
        .withColumn("thermometer_observation_1", lit("NaN"))
        .withColumn("thermometer_observation_2", lit("NaN"))
        .withColumn("thermometer_observation_3", lit("NaN"))
        .withColumn("air_pressure_degC_1", lit("NaN"))
        .withColumn("air_pressure_degC_2", lit("NaN"))
        .withColumn("air_pressure_degC_3", lit("NaN"))

      val pressureData1862FinalDF = pressureData1862TempDF.select(
        "year", "month", "day", "pressure_morning", "pressure_noon", "pressure_evening",
        "station", "pressure_unit", "barometer_temp_1", "barometer_temp_2",
        "barometer_temp_3", "thermometer_observation_1",
        "thermometer_observation_2", "thermometer_observation_3",
        "air_pressure_degC_1", "air_pressure_degC_2", "air_pressure_degC_3")

      val pressureData1938RDD = rawPressureData1938.map(x => x.split("\\s+")).map(x=>pressure_Schema.UncleanPressureSchema(x(0), x(1), x(2), x(3), x(4), x(5),x(6)))
      val pressureData1938DF = sparkSession.createDataFrame(pressureData1938RDD).drop("col1")
      val pressureData1938TempDF = pressureData1938DF
        .withColumn("station", lit("NaN"))
        .withColumn("pressure_unit", lit("hpa"))
        .withColumn("barometer_temp_1", lit("NaN"))
        .withColumn("barometer_temp_2", lit("NaN"))
        .withColumn("barometer_temp_3", lit("NaN"))
        .withColumn("thermometer_observation_1", lit("NaN"))
        .withColumn("thermometer_observation_2", lit("NaN"))
        .withColumn("thermometer_observation_3", lit("NaN"))
        .withColumn("air_pressure_degC_1", lit("NaN"))
        .withColumn("air_pressure_degC_2", lit("NaN"))
        .withColumn("air_pressure_degC_3", lit("NaN"))

      val pressureData1938finalDF = pressureData1938TempDF.select(
        "year", "month", "day", "pressure_morning", "pressure_noon", "pressure_evening",
        "station", "pressure_unit", "barometer_temp_1", "barometer_temp_2",
        "barometer_temp_3", "thermometer_observation_1",
        "thermometer_observation_2", "thermometer_observation_3",
        "air_pressure_degC_1", "air_pressure_degC_2", "air_pressure_degC_3")

      val pressureData1961RDD = rawPressureData1961.map(x => x.split("\\s+")).map(x=>pressure_Schema.PressureSchema(x(0), x(1), x(2), x(3), x(4), x(5)))
      val pressureData1961DF = sparkSession.createDataFrame(pressureData1961RDD)
      val pressureData1961TempDF = pressureData1961DF
        .withColumn("station", lit("NaN"))
        .withColumn("pressure_unit", lit("hpa"))
        .withColumn("barometer_temp_1", lit("NaN"))
        .withColumn("barometer_temp_2", lit("NaN"))
        .withColumn("barometer_temp_3", lit("NaN"))
        .withColumn("thermometer_observation_1", lit("NaN"))
        .withColumn("thermometer_observation_2", lit("NaN"))
        .withColumn("thermometer_observation_3", lit("NaN"))
        .withColumn("air_pressure_degC_1", lit("NaN"))
        .withColumn("air_pressure_degC_2", lit("NaN"))
        .withColumn("air_pressure_degC_3", lit("NaN"))

      val pressureData1961FinalDF = pressureData1961TempDF.select(
        "year", "month", "day", "pressure_morning", "pressure_noon", "pressure_evening",
        "station", "pressure_unit", "barometer_temp_1", "barometer_temp_2",
        "barometer_temp_3", "thermometer_observation_1",
        "thermometer_observation_2", "thermometer_observation_3",
        "air_pressure_degC_1", "air_pressure_degC_2", "air_pressure_degC_3")

      val manualPressureDataRDD = rawManualPressureData.map(x => x.split("\\s+")).map(x=>pressure_Schema.PressureSchema(x(0), x(1), x(2), x(3), x(4), x(5)))
      val manualPressureDataDF = sparkSession.createDataFrame(manualPressureDataRDD)
      val manualPressureTempDF = manualPressureDataDF
        .withColumn("station", lit("manual"))
        .withColumn("pressure_unit", lit("hpa"))
        .withColumn("barometer_temp_1", lit("NaN"))
        .withColumn("barometer_temp_2", lit("NaN"))
        .withColumn("barometer_temp_3", lit("NaN"))
        .withColumn("thermometer_observation_1", lit("NaN"))
        .withColumn("thermometer_observation_2", lit("NaN"))
        .withColumn("thermometer_observation_3", lit("NaN"))
        .withColumn("air_pressure_degC_1", lit("NaN"))
        .withColumn("air_pressure_degC_2", lit("NaN"))
        .withColumn("air_pressure_degC_3", lit("NaN"))

      val manualPressureDataFinalDF = manualPressureTempDF.select(
        "year", "month", "day", "pressure_morning", "pressure_noon", "pressure_evening",
        "station", "pressure_unit", "barometer_temp_1", "barometer_temp_2",
        "barometer_temp_3", "thermometer_observation_1",
        "thermometer_observation_2", "thermometer_observation_3",
        "air_pressure_degC_1", "air_pressure_degC_2", "air_pressure_degC_3")

      val autoPressureDataRDD = rawAutoPressureData.map(x => x.split("\\s+")).map(x =>pressure_Schema.PressureSchema(x(0), x(1), x(2), x(3), x(4), x(5)))
      val autoPressureDF = sparkSession.createDataFrame(autoPressureDataRDD)
      val autoPressureTempDF = autoPressureDF
        .withColumn("station", lit("Automatic"))
        .withColumn("pressure_unit", lit("hpa"))
        .withColumn("barometer_temp_1", lit("NaN"))
        .withColumn("barometer_temp_2", lit("NaN"))
        .withColumn("barometer_temp_3", lit("NaN"))
        .withColumn("thermometer_observation_1", lit("NaN"))
        .withColumn("thermometer_observation_2", lit("NaN"))
        .withColumn("thermometer_observation_3", lit("NaN"))
        .withColumn("air_pressure_degC_1", lit("NaN"))
        .withColumn("air_pressure_degC_2", lit("NaN"))
        .withColumn("air_pressure_degC_3", lit("NaN"))

      // selecting the required columns in the dataframe
      val autoPressureDataFinalDF = autoPressureTempDF.select(
        "year", "month", "day", "pressure_morning", "pressure_noon", "pressure_evening",
        "station", "pressure_unit", "barometer_temp_1", "barometer_temp_2",
        "barometer_temp_3", "thermometer_observation_1",
        "thermometer_observation_2", "thermometer_observation_3",
        "air_pressure_degC_1", "air_pressure_degC_2", "air_pressure_degC_3")

      // union of temperature data
      pressureDF = manualPressureDataFinalDF
        .union(autoPressureDataFinalDF)
        .union(pressureData1756finalDF)
        .union(pressureData1859FinalDF)
        .union(pressureData1862FinalDF)
        .union(pressureData1938finalDF)
        .union(pressureData1961FinalDF)
        .dropDuplicates()

      // hive table creation to store final pressure data
      spark.sql("""CREATE TABLE PressureData(
                  year String,
                  month String,
                  day String,
                  pressure_morning String,
                  pressure_noon String,
                  pressure_evening String,
                  station String,
                  pressure_unit String,
                  barometer_temperature_observations_1 String,
                  barometer_temperature_observations_2 String,
                  barometer_temperature_observations_3 String,
                  thermometer_observations_1 String,
                  thermometer_observations_2 String,
                  thermometer_observations_3 String,
                  air_pressure_reduced_to_0_degC_1 String,
                  air_pressure_reduced_to_0_degC_2 String,
                  air_pressure_reduced_to_0_degC_3 String)
                STORED AS PARQUET""")

      // Writing data to hive table
      pressureDF.write.mode(SaveMode.Overwrite).saveAsTable("PressureDataTable")

    } catch {
      case ex:Exception=>
        logger.error("Pressure data could not be computed",ex)

    }
    pressureDF
  }
}

