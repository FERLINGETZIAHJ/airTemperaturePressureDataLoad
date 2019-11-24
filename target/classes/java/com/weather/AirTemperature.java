package com.weather;

import org.apache.spark.sql.*;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import java.io.File;
import org.apache.spark.sql.api.java.UDF5;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.*;
import static org.apache.spark.sql.functions.*;

public class AirTemperature {
    private static final Logger log = Logger.getLogger(AirTemperature.class.getName());
    private static SparkSession spark;
    private static String directoryName = "C:\\Users\\FairyJacob\\Desktop\\Spark\\test\\";

    public static void main(String[] args) {
        //log.setLevel(Level.ERROR);
        spark = SparkSession.builder().appName("MyWeatherDataAnalysisApp").config("spark.master", "local").getOrCreate();
        System.setProperty("hadoop.home.dir", "C:\\hadoop\\bin\\winutils.exe");
        Logger.getLogger("org").setLevel(Level.OFF);
        processAirTemperatureData(directoryName);
    }

    private static void processAirTemperatureData(String directoryName) {
        try {
            Dataset<org.apache.spark.sql.Row> MergedTempDS = null;
            boolean validData = false;
            File folder = new File(directoryName);
            File[] listOfFiles = folder.listFiles();
            assert listOfFiles != null;
            for (File listOfFile : listOfFiles) {
                Dataset<Row> temperatureDS = null;
                if (listOfFile.isFile()) {
                    String filename = listOfFile.getName();
                    log.info("File Name : " + filename + " length : " + listOfFiles.length);
                    temperatureDS = readInputAndFrameDataset(spark, directoryName, filename);
                    validData = validateIfFileContainsRelevantData(filename, temperatureDS);
                } else if (listOfFile.isDirectory()) {
                    log.info("Directory " + listOfFile.getName());
                }

                if (validData && MergedTempDS == null) {
                    MergedTempDS = temperatureDS;
                } else if(validData){
                    assert temperatureDS != null;
                    MergedTempDS = temperatureDS.union(MergedTempDS);
                }else{
                    log.error("File Doesn't Contain the Expected Years Data.");
                }
            }
            assert MergedTempDS != null;
            MergedTempDS.show(50, false);

            String[] colNames = {"MAX_TEMP", "MIN_TEMP", "MEAN_TEMP"};
            Dataset<org.apache.spark.sql.Row> MergedTempDSWithoutNULL = MergedTempDS.na().fill(Double.NaN, colNames);
            Dataset<Row> validAirTemperatureDS = writeBadRecordsToHDFS(MergedTempDSWithoutNULL);

            //Categorization of Weather Data based on the Data Publisher.
            Dataset<Row> airTemperatureData = validAirTemperatureDS
                    .withColumn("PUBLISHED_CATEGORY", when(col("YEAR").between(1756, 1858), lit("SMHI_STAFF"))
                            .when(col("YEAR").between(1859, 1960), lit("IMPROVE"))
                            .when(col("YEAR").between(1961, 2012), lit("SMHI"))
                            .when((col("YEAR").between(2013, 2017).and(col("ISMANUAL").equalTo(true))), lit("MANUAL_STN"))
                            .otherwise(lit("AUTOMATIC_STN")))
                    .drop("ISMANUAL");
            //log.info(publishedCategoryDataset.count());

            //Validate if Duplicate Data Exists in the Dataset.
            boolean isduplicateExists= duplicateExists(airTemperatureData);
            if(isduplicateExists)
                airTemperatureData = airTemperatureData.distinct();
            //airTemperatureData.show(50,false);

            boolean multipleRecordsPerDayExists = checkForMultipleRecordsPerDay(spark, airTemperatureData);
            log.info(multipleRecordsPerDayExists);

            //Validate if multiple Data Exists for a day.
            if (!multipleRecordsPerDayExists)
                writeToHDFS(airTemperatureData, CommonConstants.airTemperatureHDFSFilePath);
            spark.stop();
        } catch (Exception e) {
            System.out.println("ERROR OCCURED --" + e.getMessage());
        }
    }

    public static boolean validateIfFileContainsRelevantData(String fileName, Dataset temperatureDS)
    {
        boolean validData = false;
        String[] getYears = fileName.split("_");
        int startYear = Integer.parseInt(getYears[4]);
        int endYear = Integer.parseInt(getYears[5]);
        long totalYears = endYear-startYear;
        Dataset<FieldNames> yearDS = temperatureDS
                .select(min("YEAR").alias("startYear"),max("YEAR").alias("endYear"),countDistinct("YEAR").alias("yearCount"))
                .as(Encoders.bean(FieldNames.class));
        yearDS.show(20,false);
        int startYearFromDS = yearDS.first().getStartYear();
        int endYearFromDS = yearDS.first().getEndYear();
        long totalYearsFromDS = yearDS.first().getYearCount();
        log.info("startYear -"+startYear+ " endYear -"+endYear+ "startYearFromDS -"+startYearFromDS+ " endYearFromDS -"+endYearFromDS);
        if(startYear == startYearFromDS && endYear == endYearFromDS && totalYears == totalYearsFromDS)
            validData = true;
        return validData;
    }
    /**
     * Method to read the Input File and frame Datasets.
     * Inputs : SparkSession and FileName
     * @param spark         - "Spark Session"
     * @param inputFileName - "Input File Path"
     * @return : DATASET Containing the Data from File.
     */
    public static Dataset<org.apache.spark.sql.Row> readInputAndFrameDataset(SparkSession spark, String directoryName, String inputFileName) {
        Dataset<org.apache.spark.sql.Row> temperatureDS = null;
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("YEAR", DataTypes.IntegerType, false),
                DataTypes.createStructField("MONTH", DataTypes.IntegerType, false),
                DataTypes.createStructField("DAY", DataTypes.IntegerType, false),
                DataTypes.createStructField("MORG_TEMP", DataTypes.DoubleType, false),
                DataTypes.createStructField("NOON_TEMP", DataTypes.DoubleType, false),
                DataTypes.createStructField("EVE_TEMP", DataTypes.DoubleType, true),
                DataTypes.createStructField("MAX_TEMP", DataTypes.DoubleType, true),
                DataTypes.createStructField("MIN_TEMP", DataTypes.DoubleType, true),
                DataTypes.createStructField("MEAN_TEMP", DataTypes.DoubleType, true),
                DataTypes.createStructField("_corrupt_record", DataTypes.StringType, true)
        });
        try {
            Dataset<org.apache.spark.sql.Row> temperatureRawDS = spark.read()
                    .schema(schema)
                    .format("csv")
                    .option("delimiter", "\t")
                    .load(directoryName + inputFileName).toDF();
            if (inputFileName.startsWith("stockholmA"))
                temperatureDS = temperatureRawDS.withColumn("ISMANUAL", lit(false));
            else
                temperatureDS = temperatureRawDS.withColumn("ISMANUAL", lit(true));
        } catch (Exception e) {
            log.error(e.getMessage());
        }
        assert temperatureDS != null;
        temperatureDS.show(50, false);
        return temperatureDS;
    }

    /**
     * This method is to validate if duplicate records exists, If exists the distinct values of records are returned.
     * @param airtemperatureDS - "Dataset containing all the Merged Data from TextFile"
     * @return Dataset - Distinct Dataset
     */
    public static boolean duplicateExists(Dataset<org.apache.spark.sql.Row> airtemperatureDS) {
        if (airtemperatureDS.count() == airtemperatureDS.distinct().count()) {
            log.info("No Duplicate Records found");
            return false;
        } else {
            log.info("Duplicate Records found. Taking only distinct Records.");
            return true;
        }
    }

    /**
     * This method is to checks if multiple records exists for the same day.
     * @param spark            - "Spark Session"
     * @param airtemperatureDS - "Distinct Record Dataset"
     */
    public static boolean checkForMultipleRecordsPerDay(SparkSession spark, Dataset<Row> airtemperatureDS) {
        boolean multipleRecordsPerDayExists = false;
        try {
            Dataset<org.apache.spark.sql.Row> checkForSameDateDuplicateEntries = airtemperatureDS
                    .withColumn("MultipleRecordsCheck", dense_rank().over(Window.orderBy(col("MONTH"), col("DAY"), col("YEAR"))));
            checkForSameDateDuplicateEntries.createOrReplaceTempView("checkForSameDateDuplicateEntries");
            long airtemperatureDSCount = airtemperatureDS.count();
            long distinctRank = spark.sql("select COUNT(DISTINCT(MultipleRecordsCheck)) from checkForSameDateDuplicateEntries").count();
            if (airtemperatureDSCount == distinctRank) {
                log.info("There are no Multiple Records for a day");
            } else {
                log.info("There are Multiple Records Available for a day. Please Verify the Data.");
                multipleRecordsPerDayExists = true;
            }
        } catch (Exception e) {
            log.error("Exception thrown while checking the Multiple Records Per Day Count." + e.getMessage());
        }
        return multipleRecordsPerDayExists;
    }

    /**
     * Writing the Invalid Records to HDFS to inspect later.
     * @param MergedTempDSWithoutNULL "Air Temperature Data with Corrupt Records"
     */
    public static Dataset<Row> writeBadRecordsToHDFS(Dataset<Row> MergedTempDSWithoutNULL) {
        Dataset<Row> validAirTemperatureDS = null;
        try {
            spark.udf().register("isAcceptableNullColumns", new UDF5<Integer, Integer, Integer, Double, Double, Boolean>() {
                @Override
                public Boolean call(Integer integer, Integer integer2, Integer integer3, Double aDouble, Double aDouble2) {
                    return integer != null && integer2 != null && integer3 != null && aDouble != null && aDouble2 != null;
                }
            }, DataTypes.BooleanType);

            MergedTempDSWithoutNULL = MergedTempDSWithoutNULL.withColumn("isAcceptableNullColumns", callUDF("isAcceptableNullColumns", col("YEAR"), col("MONTH"), col("DAY"), col("MORG_TEMP"), col("NOON_TEMP")));

            Dataset<Row> badRecords = MergedTempDSWithoutNULL.filter(col("_corrupt_record").isNotNull())
                    .filter(col("isAcceptableNullColumns").equalTo(false))
                    .cache();
            if (badRecords.count() > 0) {
                log.info("Writing " + badRecords.count() + " Invalid Records to HDFS.");
                badRecords.write().mode(SaveMode.Overwrite).csv(CommonConstants.airTemperatureHDFSFilePathForCorruptRecords);
                validAirTemperatureDS = MergedTempDSWithoutNULL.filter(col("isAcceptableNullColumns").equalTo(true))
                        .drop("_corrupt_record").drop("isAcceptableNullColumns");
            } else {
                validAirTemperatureDS = MergedTempDSWithoutNULL.drop("_corrupt_record", "isAcceptableNullColumns");
            }
        } catch (NullPointerException e) {
            log.error("Error thrown while writing the Corrupt/Error Records to HDFS." + e.getMessage());
        }

        return validAirTemperatureDS;
    }

    /**
     * Writing the Dataset to HDFS using Append Save Mode as many text files are read and stored.
     *
     * @param dsName       "Dataset with Air Temperature Details"
     * @param hdfsFileName "Output HDFS Path where the Air Temperature Data is to be written to be used for further processing."
     */
    public static void writeToHDFS(Dataset dsName, String hdfsFileName) {
        try {
            dsName.write().partitionBy("PUBLISHED_CATEGORY", "YEAR").mode(SaveMode.Append).parquet(hdfsFileName);
        } catch (NullPointerException e) {
            log.error("Error thrown while writing the Dataset to HDFS." + e.getMessage());
        }
    }
}