# WeatherResearch Project
## This application is to analyse Sweden Weather using pressure and temperature observations
#### Assumptions
Data is present in HDFS Location
#### Constants
This class is written to keep the hdfs location path for pressure and temperature data
#### Utilities
This utility contains functions to read data, define spark session and stop spark session
#### Load Data from HDFS using Spark and Scala
Perform data cleansing and validation and provide weather data insights
#### Data storage location
Data is stored in Hive Table
#### Build
Project is created using sbt. Any changes in the dependencies has to be updated in build.sbt
