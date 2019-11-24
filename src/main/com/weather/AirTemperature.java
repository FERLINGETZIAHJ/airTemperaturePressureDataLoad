package com.weather;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF5;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.File;

import static org.apache.spark.sql.functions.*;

public class AirTemperature {
    private static final Logger log = Logger.getLogger(AirTemperature.class.getName());
    //private static SparkSession spark;

    public static boolean fetchAirTemperatureData() {
        //log.setLevel(Level.ERROR);
        SparkSession spark = SparkSession.builder().appName("MyWeatherDataAnalysisApp").config("spark.master", "local").getOrCreate();
        System.setProperty("hadoop.home.dir", "C:\\hadoop\\bin\\winutils.exe");
        Logger.getLogger("org").setLevel(Level.OFF);
        boolean writeToHDFSComplete = processAirTemperatureData(spark);
        return writeToHDFSComplete;
    }

    private static boolean processAirTemperatureData(SparkSession spark) {
        boolean writeToHDFS = false;
        try {
            Dataset<org.apache.spark.sql.Row> MergedTempDS = null;
            boolean validData = true;
            File folder = new File(CommonConstants.airTemperatureDataDirectoryName);
            File[] listOfFiles = folder.listFiles();
            assert listOfFiles != null;
            String filename = null;
            for (File listOfFile : listOfFiles) {
                Dataset<Row> temperatureDS = null;
                if (listOfFile.isFile()) {
                    filename = listOfFile.getName();
                    log.info("Processing Air Temperature Data File :: " + filename);
                    temperatureDS = readInputAndFrameDataset(spark, CommonConstants.airTemperatureDataDirectoryName, filename);
                    //temperatureDS.show();
                    if (temperatureDS == null)
                        continue;
                    validData = CommonValidations.validateIfFileContainsRelevantData(filename, temperatureDS);
                } else if (listOfFile.isDirectory()) {
                    log.info("Directory " + listOfFile.getName());
                }

                if (validData && MergedTempDS == null) {
                    MergedTempDS = temperatureDS;
                } else if (validData) {
                    assert temperatureDS != null;
                    MergedTempDS = temperatureDS.union(MergedTempDS);
                } else {
                    log.error("File Doesn't Contain the Expected Years Data:" + filename);
                }
            }
            if (MergedTempDS != null) {

                Dataset<Row> airTemperatureData = furtherDSProcessing(spark, MergedTempDS);

                //Validate if Duplicate Data Exists in the Dataset.
                boolean isduplicateExists = CommonValidations.duplicateExists(airTemperatureData);
                if (isduplicateExists)
                    airTemperatureData = airTemperatureData.distinct();
                //airTemperatureData.show(50, false);

                boolean multipleRecordsPerDayExists = CommonValidations.checkForMultipleRecordsPerDay(spark, airTemperatureData);

                CommonValidations.validateSchemaIntegrity(MergedTempDS.drop("_corrupt_record").drop("ISMANUAL"), airTemperatureData.drop("PUBLISHED_CATEGORY"));

                //Validate if multiple Data Exists for a day.
                if (!multipleRecordsPerDayExists)
                    writeToHDFS = writeToHDFS(airTemperatureData);
            } else {
                log.info("All the Records are Corrupted or Invalid");
            }
            spark.stop();
        } catch (Exception e) {
            log.error("ERROR Occurred while processing AirTemperature to load to HDFS : " + e.getMessage());
            e.printStackTrace();
        }
        return writeToHDFS;
    }

    /**
     * Processing the Air Temperature further by categorizing it based on the Publisher of Data and also filling empty records with NaN.
     *
     * @param spark               - "Spark Session"
     * @param mergedTemperatureDS - "Dataset fetched from File"
     * @return Dataset
     */
    public static Dataset<Row> furtherDSProcessing(SparkSession spark, Dataset mergedTemperatureDS) {
        String[] colNames = {"MAX_TEMP", "MIN_TEMP", "MEAN_TEMP"};
        Dataset<org.apache.spark.sql.Row> MergedTempDSWithoutNULL = mergedTemperatureDS.na().fill(Double.NaN, colNames);

        Dataset<Row> validAirTemperatureDS = writeBadRecordsToHDFS(spark, MergedTempDSWithoutNULL);
        //Categorization of Weather Data based on the Data Publisher.
        Dataset<Row> airTemperatureData = validAirTemperatureDS
                .withColumn("PUBLISHED_CATEGORY", when(col("YEAR").between(1756, 1858), lit("SMHI_STAFF"))
                        .when(col("YEAR").between(1859, 1960), lit("IMPROVE"))
                        .when(col("YEAR").between(1961, 2012), lit("SMHI"))
                        .when((col("YEAR").between(2013, 2017).and(col("ISMANUAL").equalTo(true))), lit("MANUAL_STN"))
                        .otherwise(lit("AUTOMATIC_STN")))
                .drop("ISMANUAL");
        return airTemperatureData;
    }

    /**
     * Method to read the Input File and frame Datasets.
     * Inputs : SparkSession and FileName
     *
     * @param spark         - "Spark Session"
     * @param inputFileName - "Input File Path"
     * @return : DATASET Containing the Data from File.
     */
    public static Dataset<org.apache.spark.sql.Row> readInputAndFrameDataset(SparkSession spark, String directoryName, String inputFileName) {
        Dataset<org.apache.spark.sql.Row> temperatureDS = null;
        StructType customSchema = DataTypes.createStructType(new StructField[]{
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
                    .schema(customSchema)
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
        return temperatureDS;
    }

    /**
     * Writing the Invalid Records to HDFS to inspect later.
     *
     * @param MergedTempDSWithoutNULL "Air Temperature Data with Corrupt Records"
     *                                return Dataset
     */
    private static Dataset<Row> writeBadRecordsToHDFS(SparkSession spark, Dataset<Row> MergedTempDSWithoutNULL) {
        Dataset<Row> validAirTemperatureDS = null;
        Dataset<Row> airTemperatureDS = null;
        try {
            spark.udf().register("isAcceptableNullColumns", new UDF5<Integer, Integer, Integer, Double, Double, Boolean>() {
                @Override
                public Boolean call(Integer integer, Integer integer2, Integer integer3, Double aDouble, Double aDouble2) {
                    return integer != null && integer2 != null && integer3 != null && aDouble != null && aDouble2 != null;
                }
            }, DataTypes.BooleanType);

            airTemperatureDS = MergedTempDSWithoutNULL.withColumn("isAcceptableNullColumns", callUDF("isAcceptableNullColumns", col("YEAR"), col("MONTH"), col("DAY"), col("MORG_TEMP"), col("NOON_TEMP")));
            //airTemperatureDS.show();
            Dataset<Row> badRecords = airTemperatureDS.filter(col("_corrupt_record").isNotNull())
                    .filter(col("isAcceptableNullColumns").equalTo(false))
                    .cache();

            if (badRecords != null && badRecords.count() > 0) {
                log.info("Writing " + badRecords.count() + " Invalid Records to HDFS.");
                badRecords.write().mode(SaveMode.Overwrite).csv(CommonConstants.airTemperatureHDFSFilePathForCorruptRecords);
                validAirTemperatureDS = airTemperatureDS.filter(col("isAcceptableNullColumns").equalTo(true))
                        .drop("_corrupt_record").drop("isAcceptableNullColumns");
            } else {
                validAirTemperatureDS = MergedTempDSWithoutNULL.drop("_corrupt_record", "isAcceptableNullColumns");
            }
        } catch (NullPointerException e) {
            log.error("Error thrown while writing the Corrupt/Error Records to HDFS." + e.getMessage());
            e.printStackTrace();
        }
        return validAirTemperatureDS;
    }

    /**
     * Writing the Dataset to HDFS using Append Save Mode as many text files are read and stored.
     *
     * @param targetAirTemperatureDataset "Dataset with Air Temperature Details"
     */
    private static boolean writeToHDFS(Dataset<Row> targetAirTemperatureDataset) {
        boolean writeToHDFS = false;
        try {
            targetAirTemperatureDataset.write().partitionBy("PUBLISHED_CATEGORY", "YEAR").mode(SaveMode.Append).parquet(CommonConstants.airTemperatureHDFSFilePath);
            writeToHDFS = true;
            log.info("Data Successfully written to HDFS");
        } catch (NullPointerException e) {
            log.error("Error thrown while writing the Dataset to HDFS." + e.getMessage());
        }
        return writeToHDFS;
    }
}