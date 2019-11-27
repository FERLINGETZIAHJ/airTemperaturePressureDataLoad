package com.weather.common;

public class CommonConstants {
    public static final Double mmHgTohPa = 1.333224;
    public static final Double swedishInchTommHg = 25.4;
    public static final String airPressureDataDirectoryName = "data\\mock\\airPressure\\";
    public static final String airTemperatureDataDirectoryName = "data\\mock\\airTemperature\\";
    public static final String airTemperatureHDFSFilePath = "/user/hdfs/airTemperature/";
    public static final String airPressureHDFSFilePath = "/user/hdfs/airPressure/";
    public static final String airTemperatureHDFSFilePathForCorruptRecords = "/user/hdfs/corruptRecords/airTemperature/";
    public static final String airPressureHDFSFilePathForCorruptRecords = "/user/hdfs/corruptRecords/airPressure/";

    //Unit Tests
    public static String unitTestdirectoryName = "data\\unitTest\\";
    public static final Double hPaTommHg = 0.75006;
    public static final Double hpaToSwedish = 0.02953;
    public static final Double hpaToSwedishInchesPointOne = 0.2953;
}
