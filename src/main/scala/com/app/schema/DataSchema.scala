package com.app.schema

/**
 *
 */
class DataSchema extends Serializable {

  case class PressureSchema(
                             year:             String,
                             month:            String,
                             day:              String,
                             pressure_morning: String,
                             pressure_noon:    String,
                             pressure_evening: String)

  case class UncleanedPressureSchema(
                                      col1:            String,
                                      year:             String,
                                      month:            String,
                                      day:              String,
                                      pressure_morning: String,
                                      pressure_noon:    String,
                                      pressure_evening: String)

  case class PressureSchema_1756(
                                  year:                                 String,
                                  month:                                String,
                                  day:                                  String,
                                  pressure_morning:                     String,
                                  barometer_temp_1: String,
                                  pressure_noon:                        String,
                                  barometer_temp_2: String,
                                  pressure_evening:                     String,
                                  barometer_temp_3: String)


  case class PressureSchema_1859(
                                  year:                             String,
                                  month:                            String,
                                  day:                              String,
                                  pressure_morning:                 String,
                                  thermometer_observation_1:       String,
                                  air_pressure_degC_1: String,
                                  pressure_noon:                    String,
                                  thermometer_observation_2:       String,
                                  air_pressure_degC_2: String,
                                  pressure_evening:                 String,
                                  thermometer_observation_3:       String,
                                  air_pressure_degC_3: String)

  case class TemperatureSchema(
                                year:    String,
                                month:   String,
                                day:     String,
                                morning: String,
                                noon:    String,
                                evening: String)

  case class TemperatureSchema_1859(
                                     year:                             String,
                                     month:                            String,
                                     day:                              String,
                                     pressure_morning:                 String,
                                     temperature_min:                  String,
                                     temperature_max:                  String
                                   )

  case class TempSchema_1961_Manual_Auto(
                                          year:                 String,
                                          month:                String,
                                          day:                  String,
                                          morning:              String,
                                          noon:                 String,
                                          evening:              String,
                                          minimum:              String,
                                          maximum:              String,
                                          estimatedDiurnalMean: String)

  case class UncleanTemperatureSchema(
                                       extra:   String,
                                       year:    String,
                                       month:   String,
                                       day:     String,
                                       morning: String,
                                       noon:    String,
                                       evening: String)
}
