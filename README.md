# Atmosphere Temperature and Pressure Data Load

Use Case Description:

Need to upload the Atomsphere Temperature and Pressure Data to HDFS after performing necessary cleansing and ensuring data integrity, making it available for Data Analytics for further processing.

# Getting Started

The Main Objective:

1. To read the Air Temperature and Air Pressure data (Text File, without headers and is tab seperated).
2. To do necessary Cleansing of Data.
3. Ensure Data Integrity.
4. Load the neccessary details to HDFS.

## Steps to Build and Run Code:

1. Clone the GitHub project and create as a Maven Project.
2. Perform Maven Clean and Install(either using cmd or using IDE).
3. On Unit Test Success and Build Success, the jar will be generated in the target folder.
4. Place the jar ns the desired location and run using spark-submit command.
Ex: spark-submit --class com.weather.main.LoadWeatherDataToHDFS --master yarn --deploy-mode client /home/user/weatherData.jar 

# Module Description:

The Data is read, processed and written to HDFS. The Steps are explained as below.

## Air Temperature Data:

1. The Data is read from Source, Headers were added and relevant data validation is done.
2. Validation of Corrupt Records is done and the Corrupt Records are written to a seperate folder in HDFS.
3. Categorization of Data based on the Data Publisher(SMHI, IMPROVE, MANUAL etc) is done.
4. Missing/Null Records are filled as NaN.
5. Validation for Duplicate Records are done.
6. Then the Data is written to HDFS, If Duplicates are found then Distinct Values are taken and written to HDFS.

## Air Pressure Data:

1. The Data is read from Source, Headers were added and relevant data validation is done.
2. Validation of Corrupt Records is done and the Corrupt Records are written to a seperate folder in HDFS.
3. Categorization of Data based on Year Range is done.
4. Conversion of Units to hPa is done so as to maintain Homogenity of data
      eg: mmHg/Swedish inches Observations are converted to hPA
5. Validation for Duplicate Records are done.
6. Then the Data is written to HDFS, If Duplicates are found then Distinct Values are taken and written to HDFS.

Note: 

Common Functionalities/Constants are grouped in CommonValidations and CommonConstants Class

Mock Data is available in Data Folder.

# Unit Tests:

There are totally 10 Test Cases performed as part of Unit Tests. They are as below.

### Unit Tests for Air Temperature Data:

1. Validate if the Input File Contains only the relevant Data.
2. Validate if Duplicate Records Exists.
3. Validate if there are more than one record for a Day.
4. Validate if Schema is the same between Source and Target.

### Unit Tests for Air Pressure Data:

1. Validate if the Input File Contains only the relevant Data.
2. Validate if Duplicate Records Exists.
3. Validate if there are more than one record for a Day.
4. Validate if Schema is same between Source and Target.
5. Validate the Unit Conversion(all the pressure data should be in hPa)
6. Validate the Air Pressure Range(checking if the Pressure Data is valid)

## Pre-requisite:

Apache Spark : 2.3.2, 
Java : Jdk 1.8, 
Apache Hadoop : 2.7, 
Maven Compiler : 2.3.2, 
Junit : 1.4.1, 
IDE Used : IntelliJ, 
Data Downloaded:
  https://bolin.su.se/data/stockholm/raw_individual_temperature_observations.php
  https://bolin.su.se/data/stockholm/barometer_readings_in_original_units.php
   
## Author
Ferlin Getziah J
