package com.weather;

import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;

public class LoadWeatherDataToHDFS {

    private static final Logger log = Logger.getLogger(LoadWeatherDataToHDFS.class.getName());

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder().appName("MyAirPressureDataApp").config("spark.master", "local").getOrCreate();
        System.setProperty("hadoop.home.dir", "C:\\hadoop\\bin\\winutils.exe");
        /*SparkSession spark = SparkSession.builder()
                .appName("MyWeatherDataAnalysisApp")
                .getOrCreate();*/

        boolean temperatureDataLoadedComplete, pressureDataLoadedComplete;

        log.info("Loading Air Temperature Data to HDFS.");
        temperatureDataLoadedComplete = AirTemperature.fetchAirTemperatureData(spark);
        if (temperatureDataLoadedComplete)
            log.info("Air Temperature Data Successfully loaded to HDFS.");
        else
            log.info("Error loading Air Temperature Data to HDFS.");

        log.info("Loading Air Pressure Data to HDFS.");
        pressureDataLoadedComplete = AirPressure.fetchAirPressureData(spark);
        if (pressureDataLoadedComplete)
            log.info("Air Pressure Data Successfully loaded to HDFS.");
        else
            log.info("Error loading Air Pressure Data to HDFS.");

        spark.stop();
    }
}
