package com.weather;

import org.apache.spark.sql.*;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.*;

import static org.apache.spark.sql.functions.*;

import scala.collection.JavaConversions;
import scala.collection.Seq;

public class AirPressure {
    private static final Logger log = Logger.getLogger(AirPressure.class.getName());

    public static boolean fetchAirPressureData() {
        //log.setLevel(Level.ERROR);
        SparkSession spark = SparkSession.builder().appName("MyAirPressureDataApp").config("spark.master", "local").getOrCreate();
        System.setProperty("hadoop.home.dir", "C:\\hadoop\\bin\\winutils.exe");
        Logger.getLogger("org").setLevel(Level.OFF);
        boolean isWriteToHDFSComplete = processAirPressureData(spark, CommonConstants.airPressureDataDirectoryName);
        return isWriteToHDFSComplete;
    }

    private static boolean processAirPressureData(SparkSession spark, String directoryName) {
        boolean writeToHDFSFlag = false;
        try {
            Dataset<Row> MergedAirPressureDS = null;
            Dataset<Row> FinalAirPressureDataset = null;
            boolean validData = false;
            File folder = new File(directoryName);
            File[] listOfFiles = folder.listFiles();
            assert listOfFiles != null;
            String filename = null;
            for (File listOfFile : listOfFiles) {
                Dataset<Row> airPressureDS = null;
                if (listOfFile.isFile()) {
                    filename = listOfFile.getName();
                    log.info("Processing Air Pressure Data File : " + filename);
                    String rangeOfYears = rangeMapping(filename);
                    //log.info("rangeOfYears: " + rangeOfYears);
                    Dataset rawAirPressureDS = read3TimesObsInputAndFrameDataset(spark, directoryName, filename, getCustomStructType(rangeOfYears));
                    if (rawAirPressureDS == null)
                        continue;
                    validData = CommonValidations.validateIfFileContainsRelevantData(filename, rawAirPressureDS);
                    if (validData)
                        airPressureDS = tohPaConversion(rawAirPressureDS, rangeOfYears, getRequiredSelectColumnsSeq());
                } else if (listOfFile.isDirectory()) {
                    log.info("Directory " + listOfFile.getName());
                }
                if (airPressureDS != null && MergedAirPressureDS == null) {
                    MergedAirPressureDS = airPressureDS;
                } else if (airPressureDS != null) {
                    MergedAirPressureDS = airPressureDS.union(MergedAirPressureDS);
                } else {
                    log.error("File Doesn't Contain the Expected Years Data:" + filename);
                }
            }
            if (MergedAirPressureDS != null) {
                Dataset<org.apache.spark.sql.Row> MergedTempDSWithoutNULL = MergedAirPressureDS.na().fill(Double.NaN);
                MergedTempDSWithoutNULL.cache();

                boolean duplicateFlag = CommonValidations.duplicateExists(MergedTempDSWithoutNULL);

                if (duplicateFlag)
                    FinalAirPressureDataset = MergedTempDSWithoutNULL.distinct();
                else
                    FinalAirPressureDataset = MergedTempDSWithoutNULL;

                writeToHDFSFlag = writePressureDataToHDFS(FinalAirPressureDataset);
            } else {
                log.info("All the Records are Corrupted or Invalid");
            }
            spark.stop();
        } catch (Exception e) {
            log.info("Exception Occurred While Processing Pressure Data- " + e.getMessage());
            e.printStackTrace();
        }
        return writeToHDFSFlag;
    }

    public static String rangeMapping(String inputFileName) {
        HashMap<String, String> hmap = new HashMap<String, String>();

        hmap.put("stockholm_barometer_1756_1858.txt", "1756_1858");
        hmap.put("stockholm_barometer_1859_1861.txt", "1859_1861");
        hmap.put("stockholm_barometer_1862_1937.txt", "1862_1937");
        hmap.put("stockholm_barometer_1938_1960.txt", "1938_1960");
        hmap.put("stockholm_barometer_1961_2012.txt", "1961_2012");
        hmap.put("stockholm_barometer_2013_2017.txt", "MAN_2013_2017");
        hmap.put("stockholmA_barometer_2013_2017.txt", "AUTO_2013_2017");

        return hmap.get(inputFileName);
    }

    public static Seq<Column> getRequiredSelectColumnsSeq() {
        List<Column> requiredSelectColumns = new ArrayList<Column>();
        requiredSelectColumns.add(col("YEAR"));
        requiredSelectColumns.add(col("MONTH"));
        requiredSelectColumns.add(col("DAY"));
        requiredSelectColumns.add(col("OBSERVATION1"));
        requiredSelectColumns.add(col("OBSERVATION2"));
        requiredSelectColumns.add(col("OBSERVATION3"));
        requiredSelectColumns.add(col("SPLIT_RANGE"));

        return JavaConversions.asScalaBuffer(requiredSelectColumns).seq();

    }

    public static StructType getCustomStructType(String rangeMapping) {
        if (rangeMapping.equalsIgnoreCase("1859_1861")) {
            return DataTypes.createStructType(new StructField[]{
                    DataTypes.createStructField("YEAR", DataTypes.IntegerType, false),
                    DataTypes.createStructField("MONTH", DataTypes.IntegerType, false),
                    DataTypes.createStructField("DAY", DataTypes.IntegerType, false),
                    DataTypes.createStructField("BARO_OBS1", DataTypes.DoubleType, false),
                    DataTypes.createStructField("THERMO_OBS1", DataTypes.DoubleType, false),
                    DataTypes.createStructField("REDUCED_AIR_PRESSURE1", DataTypes.DoubleType, true),
                    DataTypes.createStructField("BARO_OBS2", DataTypes.DoubleType, false),
                    DataTypes.createStructField("THERMO_OBS2", DataTypes.DoubleType, false),
                    DataTypes.createStructField("REDUCED_AIR_PRESSURE2", DataTypes.DoubleType, true),
                    DataTypes.createStructField("BARO_OBS3", DataTypes.DoubleType, false),
                    DataTypes.createStructField("THERMO_OBS3", DataTypes.DoubleType, false),
                    DataTypes.createStructField("REDUCED_AIR_PRESSURE3", DataTypes.DoubleType, true),
                    DataTypes.createStructField("_corrupt_record", DataTypes.StringType, true)
            });
        } else if (rangeMapping.equalsIgnoreCase("1756_1858")) {
            return DataTypes.createStructType(new StructField[]{
                    DataTypes.createStructField("YEAR", DataTypes.IntegerType, false),
                    DataTypes.createStructField("MONTH", DataTypes.IntegerType, false),
                    DataTypes.createStructField("DAY", DataTypes.IntegerType, false),
                    DataTypes.createStructField("BARO_OBS1", DataTypes.DoubleType, false),
                    DataTypes.createStructField("BARO_TEMP_OBS1", DataTypes.DoubleType, false),
                    DataTypes.createStructField("BARO_OBS2", DataTypes.DoubleType, true),
                    DataTypes.createStructField("BARO_TEMP_OBS2", DataTypes.DoubleType, false),
                    DataTypes.createStructField("BARO_OBS3", DataTypes.DoubleType, false),
                    DataTypes.createStructField("BARO_TEMP_OBS3", DataTypes.DoubleType, true),
                    DataTypes.createStructField("_corrupt_record", DataTypes.StringType, true)
            });
        } else {
            return DataTypes.createStructType(new StructField[]{
                    DataTypes.createStructField("YEAR", DataTypes.IntegerType, false),
                    DataTypes.createStructField("MONTH", DataTypes.IntegerType, false),
                    DataTypes.createStructField("DAY", DataTypes.IntegerType, false),
                    DataTypes.createStructField("OBSERVATION1", DataTypes.DoubleType, false),
                    DataTypes.createStructField("OBSERVATION2", DataTypes.DoubleType, false),
                    DataTypes.createStructField("OBSERVATION3", DataTypes.DoubleType, true),
                    DataTypes.createStructField("_corrupt_record", DataTypes.StringType, true)
            });
        }
    }

    /**
     * Method to read the Input File and frame Datasets.
     * Inputs : SparkSession and FileName
     *
     * @param spark         "Spark Session"
     * @param inputFileName "Input File Name"
     * @return : DATASET
     */
    public static Dataset<Row> read3TimesObsInputAndFrameDataset(SparkSession spark, String directoryName, String inputFileName, StructType customSchema) {
        Dataset<Row> validPressureRawDS = null;
        try {
            Dataset<org.apache.spark.sql.Row> pressureRawDS = spark.read()
                    .schema(customSchema)
                    .format("csv")
                    .option("delimiter", "\t")
                    .load(directoryName + inputFileName).toDF();
            //pressureRawDS.show();
            validPressureRawDS = writeBadRecordsToHDFS(pressureRawDS);
        } catch (Exception e) {
            log.error("Error Occurred while reading the Data from File : " + e.getMessage());
        }
        return validPressureRawDS;
    }

    /**
     * Writing the Invalid Records to HDFS to inspect later.
     *
     * @param pressureRawDS "Air Temperature Data with Corrupt Records"
     */
    private static Dataset<Row> writeBadRecordsToHDFS(Dataset<Row> pressureRawDS) {
        Dataset<Row> validAirPressureDS = null;
        try {
            Dataset<Row> badRecords = pressureRawDS.filter(col("_corrupt_record").isNotNull())
                    .cache();
            if (badRecords != null && badRecords.count() > 0) {
                log.info("Writing " + badRecords.count() + " Invalid Records to HDFS.");
                badRecords.write().mode(SaveMode.Overwrite).csv(CommonConstants.airTemperatureHDFSFilePathForCorruptRecords);
                validAirPressureDS = pressureRawDS.filter(col("_corrupt_record").isNull())
                        .drop("_corrupt_record");
            } else {
                validAirPressureDS = pressureRawDS.drop("_corrupt_record");
            }
        } catch (NullPointerException e) {
            log.error("Error thrown while writing the Corrupt/Error Records to HDFS." + e.getMessage());
        }

        return validAirPressureDS;
    }

    /**
     * This Method is used to Convert all the units of Air Pressure Reading to hPa
     *
     * @param validPressureRawDS    - Air Pressure Data
     * @param rangeOfYears          - Split in years
     * @param requiredSelectColumns - Columns that are required while writing to HDFS
     * @return dataset
     */
    public static Dataset<Row> tohPaConversion(Dataset<Row> validPressureRawDS, String rangeOfYears, Seq<Column> requiredSelectColumns) {
        Double conversionUnit = null;
        List<String> columnNames = new ArrayList<>();
        Dataset<Row> airPressureDS = validPressureRawDS.withColumn("SPLIT_RANGE", lit(rangeOfYears));
        log.info(rangeOfYears);
        try {
            if (rangeOfYears.equalsIgnoreCase("1862_1937")) {
                columnNames.add("OBSERVATION1");
                columnNames.add("OBSERVATION2");
                columnNames.add("OBSERVATION3");
                conversionUnit = CommonConstants.mmHgTohPa;
                airPressureDS = airPressureTohPaConversion(airPressureDS, columnNames, conversionUnit, requiredSelectColumns);
            }
            if (rangeOfYears.equalsIgnoreCase("1756_1858")) {
                columnNames.add("BARO_OBS1");
                columnNames.add("BARO_OBS2");
                columnNames.add("BARO_OBS3");
                conversionUnit = CommonConstants.swedishInchTommHg * CommonConstants.mmHgTohPa;
                airPressureDS = airPressureTohPaConversion(airPressureDS, columnNames, conversionUnit, requiredSelectColumns);
            }
            if (rangeOfYears.equalsIgnoreCase("1859_1861")) {
                columnNames.add("REDUCED_AIR_PRESSURE1");
                columnNames.add("REDUCED_AIR_PRESSURE2");
                columnNames.add("REDUCED_AIR_PRESSURE3");
                conversionUnit = CommonConstants.swedishInchTommHg * CommonConstants.mmHgTohPa * 0.1;
                airPressureDS = airPressureTohPaConversion(airPressureDS, columnNames, conversionUnit, requiredSelectColumns);
            }
        } catch (Exception e) {
            log.info("Exception occurred when converting the Air Pressure Units to hPa Units : " + e.getMessage());
        }
        return airPressureDS;
    }

    /**
     * This Method is used to Convert the Barometer Observations into hPa.
     * This handles the below Scenarios:
     * mmHg to hPa - done by multiplying the Air Pressure Observations in mmHg with Constant Value (1.333224) - Formula = mmHg * 1.33224
     * swedishInches (29.69mm) to hPa - done by converting SwedishInches into mmHg and then finally into hPa. - Formula = swedishInches * 25.4 * 1.33224
     * 0.1 * swedishInches (2.969mm) to hPa - done by converting SwedishInches into mmHg and then finally into hPa. - Formula = 0.1 * swedishInches * 25.4 * 1.33224
     *
     * @param airPressureDS - "The Dataset from the provided txtFile containing the Barometer Readings"
     * @param ColNames      - "Column Names which is to be converted into hPa"
     * @param unit          - "Unit Conversion values from which the Conversion is to be done to hPa"
     * @param selectColumns - "Required Columns for further processing"
     * @return Dataset - "Output Dataset Containing the Observation Details in hPa"
     */
    private static Dataset<Row> airPressureTohPaConversion(Dataset<Row> airPressureDS, List<String> ColNames, Double unit, Seq<Column> selectColumns) {
        return airPressureDS
                .withColumn("OBSERVATION1", when(col(ColNames.get(0)).isNotNull(), (col(ColNames.get(0)).multiply(unit)).cast(new DecimalType(38, 1))).otherwise(col(ColNames.get(0))))
                .withColumn("OBSERVATION2", when(col(ColNames.get(1)).isNotNull(), (col(ColNames.get(1)).multiply(unit)).cast(new DecimalType(38, 1))).otherwise(col(ColNames.get(1))))
                .withColumn("OBSERVATION3", when(col(ColNames.get(2)).isNotNull(), (col(ColNames.get(2)).multiply(unit)).cast(new DecimalType(38, 1))).otherwise(col(ColNames.get(2))))
                .select(selectColumns);
    }

    /**
     * Writing the Air Pressure Data to HDFS using Append Save Mode as many text files are read and stored.
     *
     * @param airPressureDSName - "AirPressureDataset to be written to HDFS."
     */
    private static boolean writePressureDataToHDFS(Dataset<Row> airPressureDSName) {
        boolean writeToHDFS = false;
        log.info("Writing the Pressure Data to HDFS");
        try {
            airPressureDSName.write().partitionBy("SPLIT_RANGE", "YEAR").mode(SaveMode.Append).parquet(CommonConstants.airPressureHDFSFilePath);
            writeToHDFS = true;
        } catch (NullPointerException e) {
            log.error("Error thrown while writing the Dataset to HDFS." + e.getMessage());
        }
        return writeToHDFS;
    }
}