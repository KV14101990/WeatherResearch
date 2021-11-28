package com.app.compute

import com.app.constants.Constants
import com.app.schema.DataSchema
import com.app.utilities.SparkComputation
import org.apache.log4j.LogManager
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.lit

import java.io.FileNotFoundException
class TemperatureComputation extends SparkComputation{

   val logger = LogManager.getLogger(this.getClass.getName)

    def pressureCompute(sparkSession: SparkSession): Unit = {

      val temperature_Schema = new DataSchema
      try{

        //Reading Pressure Data 1756
        val rawManualStationTempData = sparkSession
          .sparkContext
          .textFile(Constants.Temperature_Manual_Station)

          //---------------------------Manual Station Temperature Data---------------------------------

          // Reading input data
          val manualStationTempRDD = rawManualStationTempData.map(x => x.split("\\s+")).map(x=>temperature_Schema.TempSchema_1961_Manual_Auto(

                x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8)))
          val manualStationTempDF = sparkSession.createDataFrame(manualStationTempRDD)

          val manualStationTempFinalDF = manualStationTempDF.withColumn("station", lit("manual"))


          //-------------------------Automatic Station Temperature Data------------------------------------

          val rawAutoStationTempData = sparkSession
            .sparkContext
            .textFile(Constants.Temperature_Automatic_Station)
        val autoStationTempRDD= rawAutoStationTempData.map(item => item.split("\\s+"))
            .map(x => temperature_Schema.TempSchema_1961_Manual_Auto(
              x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8)))
          val autoStationTempDF = sparkSession.createDataFrame(autoStationTempRDD)

          // Add station column
          val automaticStationFinalDF = autoStationTempDF.withColumn("station", lit("automatic"))


          //-----------------------------Space contained temperature data----------------------------------

          //Reading input data
          val rawTemp1756Data = sparkSession
            .sparkContext
            .textFile(Constants.Temperature_1756)
        val temp1756RDD= rawAutoStationTempData.map(item => item.split("\\s+"))
          .map(x => temperature_Schema.UncleanTemperatureSchema(x(0), x(1), x(2), x(3), x(4), x(5), x(6)))
          val temp1756DF = sparkSession.createDataFrame(temp1756RDD)

          // Add or remove necessary columns in the dataframe
          val temp1756FinalDF = temp1756DF
            .drop("extra")
            .withColumn("minimum", lit("NaN"))
            .withColumn("maximum", lit("NaN"))
            .withColumn("estimatedDiurnalMean", lit("NaN"))
            .withColumn("station", lit("NaN"))


          //--------------------------------------------------------

          //Reading input data
          val temperatureData1859 = sparkSession
          .sparkContext
          .textFile(Constants.Temperature_1859)
            .map(item => item.split("\\s+"))
            .map(x => temperature_Schema.TemperatureSchema_1859(x(0), x(1), x(2), x(3), x(4), x(5)))

//          //creating dataframe and adding necessary columns
//          val temperatureDataTempDF = sparkSession.createDataFrame(temperatureDataRDD)
//          val temperatureDataDF = temperatureDataTempDF
//            .withColumn("minimum", lit("NaN"))
//            .withColumn("maximum", lit("NaN"))
//            .withColumn("estimatedDiurnalMean", lit("NaN"))
//            .withColumn("station", lit("NaN"))
//
//          // Joining all the input data to make as one final data frame
//          val temperatureDF = manualStationDF
//            .union(autoStationDF)
//            .union(uncleanTempCleansedDF)
//            .union(temperatureDataDF)

//
//          //----------------------------Save temperature data to hive table-----------------------------
//
//
//          // Creating hive table
//          spark.sql("""CREATE TABLE TemperatureData(
//        year String,
//        month String,
//        day String,
//        morning String,
//        noon String,
//        evening String,
//        minimum String,
//        maximum String,
//        estimated_diurnal_mean String,
//        station String)
//      STORED AS PARQUET""")
//
//
//          temperatureDF.write.mode(SaveMode.Overwrite).saveAsTable("TemperatureData")
//
//
//          //---------------------------------------Data Analysis---------------------------------
//
//          val temperatureDFOutputCount =  temperatureDF.count()
//          logger.info("Transformed pressure output data count is " + temperatureDFOutputCount)
//          logger.info("Hive data count for pressure data " + spark.sql("SELECT count(*) as count FROM PressureData").show(false))

        } catch {
          case _: FileNotFoundException =>
            logger.error("File not found")

        }
        }
      }