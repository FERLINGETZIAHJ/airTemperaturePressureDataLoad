package com.weather;

import org.apache.log4j.Logger;

public class LoadWeatherDataToHDFS {

    private static final Logger log = Logger.getLogger(LoadWeatherDataToHDFS.class.getName());
    public static void main (String[] args)
    {
        boolean dataLoadedComplete = false;
        log.info("Loading Air Temperature Data to HDFS.");
        dataLoadedComplete = AirPressure.fetchAirPressureData();
            dataLoadedComplete = AirTemperature.fetchAirTemperatureData();
            if(dataLoadedComplete) {
                log.info("Air Temperature Data Successfully loaded to HDFS.");
                log.info("Loading Air Pressure Data to HDFS.");
                dataLoadedComplete = AirPressure.fetchAirPressureData();
                if (dataLoadedComplete)
                    log.info("Air Pressure Data Successfully loaded to HDFS.");
                else
                    log.info("Error loading Air Pressure Data to HDFS.");
            }else
                log.info("Error loading Air Temperature Data to HDFS.");
    }
}
