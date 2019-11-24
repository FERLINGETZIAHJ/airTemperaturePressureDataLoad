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
    private static String directoryName = "C:\\Users\\FairyJacob\\Desktop\\Spark\\airPressure\\";

    public static void main(String[] args) {
        try {
            //log.setLevel(Level.ERROR);
            SparkSession spark = SparkSession.builder().appName("MyAirPressureDataApp").config("spark.master", "local").getOrCreate();
            System.setProperty("hadoop.home.dir", "C:\\hadoop\\bin\\winutils.exe");
            Logger.getLogger("org").setLevel(Level.OFF);

            Dataset<Row> MergedAirPressureDS = null;
            File folder = new File(directoryName);
            File[] listOfFiles = folder.listFiles();
            assert listOfFiles != null;
            for (File listOfFile : listOfFiles) {
                Dataset<Row> airPressureDS = null;
                if (listOfFile.isFile()) {
                    String filename = listOfFile.getName();
                    log.info("File Name : " + filename + " length : " + listOfFiles.length);
                    String rangeOfYears = rangeMapping(filename);
                    log.info("rangeOfYears: " + rangeOfYears);
                    airPressureDS = read3TimesObsInputAndFrameDataset(spark, filename, rangeOfYears, getCustomStructType(rangeOfYears), getRequiredSelectColumnsSeq());
                } else if (listOfFile.isDirectory()) {
                    log.info("Directory " + listOfFile.getName());
                }
                if (MergedAirPressureDS == null) {
                    MergedAirPressureDS = airPressureDS;
                } else {
                    assert airPressureDS != null;
                    MergedAirPressureDS = airPressureDS.union(MergedAirPressureDS);
                }
            }
            assert MergedAirPressureDS != null;
            Dataset<org.apache.spark.sql.Row> MergedTempDSWithoutNULL = MergedAirPressureDS.na().fill(Double.NaN);
            MergedTempDSWithoutNULL.cache();
            Dataset<Row> FinalAirPressureDataset = validateDuplicateRecords(MergedTempDSWithoutNULL);
            writeToHDFS(FinalAirPressureDataset);
            spark.stop();
        } catch (Exception e) {
            log.info("Exception Occurred - "+e.getMessage());
        }
    }

    private static String rangeMapping(String inputFileName)
    {
        HashMap<String, String> hmap = new HashMap<String, String>();

        hmap.put("stockholm_barometer_1756_1858.txt","1756_1858");
        hmap.put("stockholm_barometer_1859_1861.txt","1859_1861");
        hmap.put("stockholm_barometer_1862_1937.txt","1862_1937");
        hmap.put("stockholm_barometer_1938_1960.txt","1938_1960");
        hmap.put("stockholm_barometer_1961_2012.txt","1961_2012");
        hmap.put("stockholm_barometer_2013_2017.txt","MAN_2013_2017");
        hmap.put("stockholmA_barometer_2013_2017.txt","AUTO_2013_2017");

        return hmap.get(inputFileName);
    }
    private static Seq<Column> getRequiredSelectColumnsSeq()
    {
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
    private static StructType getCustomStructType(String rangeMapping)
    {
        if(rangeMapping.equalsIgnoreCase("1859_1861")) {
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
        } else if(rangeMapping.equalsIgnoreCase("1756_1858")) {
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
        }else{
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
     * @param spark "Spark Session"
     * @param inputFileName "Input File Name"
     * @return : DATASET
     */
    private static Dataset<Row> read3TimesObsInputAndFrameDataset(SparkSession spark, String inputFileName, String rangeOfYears, StructType customSchema, Seq<Column> requiredSelectColumns) {
        Dataset<Row> airPressureDS = null;
        try {
            Dataset<org.apache.spark.sql.Row> pressureRawDS = spark.read()
                    .schema(customSchema)
                    .format("csv")
                    .option("delimiter", "\t")
                    .load(directoryName + inputFileName).toDF();

            Dataset<Row> validPressureRawDS = writeBadRecordsToHDFS(pressureRawDS);

            airPressureDS = validPressureRawDS.withColumn("SPLIT_RANGE",lit(rangeOfYears));
            if(rangeOfYears.equalsIgnoreCase("1862_1937")) {
                airPressureDS = mmhgTohPaConversion(airPressureDS);
                airPressureDS.show();
            }
            if(rangeOfYears.equalsIgnoreCase("1859_1861")){
                airPressureDS = swedishInchMMTommhgTohPaConversion(airPressureDS, requiredSelectColumns);
                airPressureDS.show();
            }
            if(rangeOfYears.equalsIgnoreCase("1756_1858")){
                airPressureDS = swedishInchesTohPaConversion(airPressureDS, requiredSelectColumns);
                airPressureDS.show();
            }
        } catch (Exception e) {
            log.error("Error Occurred while reading the Data from File : "+e.getMessage());
        }
        return airPressureDS;
    }

    /**
     * Writing the Invalid Records to HDFS to inspect later.
     * @param pressureRawDS "Air Temperature Data with Corrupt Records"
     */
    private static Dataset<Row> writeBadRecordsToHDFS(Dataset<Row> pressureRawDS) {
        Dataset<Row> validAirPressureDS = null;
        try {
            Dataset<Row> badRecords = pressureRawDS.filter(col("_corrupt_record").isNotNull())
                    .cache();
            if(badRecords.count()>0)
            {
                log.info("Writing "+badRecords.count()+" Invalid Records to HDFS.");
                //badRecords.write().mode(SaveMode.Overwrite).csv(CommonConstants.airTemperatureHDFSFilePathForCorruptRecords);
                validAirPressureDS =  pressureRawDS.filter(col("_corrupt_record").isNull())
                        .drop("_corrupt_record");
            }
            else
            {
                validAirPressureDS = pressureRawDS.drop("_corrupt_record");
            }
        } catch (NullPointerException e) {
            log.error("Error thrown while writing the Corrupt/Error Records to HDFS." + e.getMessage());
        }

        return validAirPressureDS;
    }
    /**
     * This Method is to Convert the Observations in mmHg into hPa. This is done by multiplying the Air Pressure Observations in mmHg with Constant Value (1.333224)
     * Formula = mmHg * 1.33224
     * @param airPressureDS "Air Pressure Dataset with Observations in mmHg"
     * @return Dataset "Output Dataset Containing the Observation Details in hPa"
     */
    private static Dataset<Row> mmhgTohPaConversion(Dataset<Row> airPressureDS) {
        return airPressureDS
                .withColumn("OBSERVATION1",col("OBSERVATION1").multiply(CommonConstants.mmHgTohPa).cast(new DecimalType(38,1)))
                .withColumn("OBSERVATION2",col("OBSERVATION2").multiply(CommonConstants.mmHgTohPa).cast(new DecimalType(38,1)))
                .withColumn("OBSERVATION3",col("OBSERVATION3").multiply(CommonConstants.mmHgTohPa).cast(new DecimalType(38,1)));
    }

    /**
     * This method is to Convert the Barometer Readings from SwedishInches into mmHg and then finally into hPa.
     * Formula = swedishInches * 25.4 * 1.33224
     * @param airPressureDS - "The Dataset from the provided txtFile containing the Barometer Readings in Swedish Inches (0.2969mm)"
     * @param selectColumns -"Required Columns for further processing"
     * @return Dataset - "Output Dataset Containing the Observation Details in hPa"
     */
    private static Dataset<Row> swedishInchesTohPaConversion(Dataset<Row> airPressureDS, Seq<Column> selectColumns) {
        return airPressureDS
                .withColumn("OBSERVATION1",when(col("BARO_OBS1").isNotNull(),(col("BARO_OBS1").multiply(CommonConstants.swedishInchTommHg)).multiply(CommonConstants.mmHgTohPa).cast(new DecimalType(38,1))).otherwise(col("BARO_OBS1")))
                .withColumn("OBSERVATION2",when(col("BARO_OBS2").isNotNull(),(col("BARO_OBS2").multiply(CommonConstants.swedishInchTommHg)).multiply(CommonConstants.mmHgTohPa).cast(new DecimalType(38,1))).otherwise(col("BARO_OBS2")))
                .withColumn("OBSERVATION3",when(col("BARO_OBS3").isNotNull(),(col("BARO_OBS3").multiply(CommonConstants.swedishInchTommHg)).multiply(CommonConstants.mmHgTohPa).cast(new DecimalType(38,1))).otherwise(col("BARO_OBS3")))
                .select(selectColumns);
    }

    /**
     * This method is to Convert the Barometer Readings in 0.1 * SwedishInches into mmHg and then finally into hPa.
     * @param airPressureDS - "The Dataset from the provided txtFile containing the Barometer Readings in 0.1*Swedish Inches (2.969mm)"
     * @param selectColumns - "Required Columns for further processing"
     * @return Dataset - "Output Dataset Containing the Observation Details in hPa"
     */
    private static Dataset<Row> swedishInchMMTommhgTohPaConversion(Dataset<Row> airPressureDS, Seq<Column> selectColumns) {
        return airPressureDS
                .withColumn("OBSERVATION1",when(col("BARO_OBS1").isNotNull(),(col("BARO_OBS1").multiply(CommonConstants.swedishInchTommHg)).multiply(CommonConstants.mmHgTohPa).multiply(0.1).cast(new DecimalType(38,1))).otherwise(col("BARO_OBS1")))
                .withColumn("OBSERVATION2",when(col("BARO_OBS2").isNotNull(),(col("BARO_OBS2").multiply(CommonConstants.swedishInchTommHg)).multiply(CommonConstants.mmHgTohPa).multiply(0.1).cast(new DecimalType(38,1))).otherwise(col("BARO_OBS2")))
                .withColumn("OBSERVATION3",when(col("BARO_OBS3").isNotNull(),(col("BARO_OBS3").multiply(CommonConstants.swedishInchTommHg)).multiply(CommonConstants.mmHgTohPa).multiply(0.1).cast(new DecimalType(38,1))).otherwise(col("BARO_OBS3")))
                .select(selectColumns);
    }
    /**
     * This method is to validate if duplicate records exists, If exists the distinct values of records are returned.
     * @param airPressureDS - "Dataset with Air Pressure Data"
     * @return Dataset - "Distinct Record Dataset"
     */
    private static Dataset<org.apache.spark.sql.Row> validateDuplicateRecords(Dataset<org.apache.spark.sql.Row> airPressureDS) {
        log.info("Inside validateDuplicateRecords CHECK"+airPressureDS.count());
        log.info("Inside validateDuplicateRecords DISTINCT CHECK"+airPressureDS.distinct().count());
        if (airPressureDS.count() == airPressureDS.distinct().count()) {
            log.info("No Duplicate Records found");
            return airPressureDS;
        } else {
            log.info("Duplicate Records found. Taking only distinct Records.");
            return airPressureDS.distinct();
        }
    }
    /**
     * Writing the Air Pressure Data to HDFS using Append Save Mode as many text files are read and stored.
     * @param airPressureDSName - "AirPressureDataset to be written to HDFS."
     */
    private static void writeToHDFS(Dataset<Row> airPressureDSName) {
        log.info("Writing the Pressure Data to HDFS");
        try {
            airPressureDSName.write().partitionBy("SPLIT_RANGE", "YEAR").mode(SaveMode.Append).parquet(CommonConstants.airPressureHDFSFilePath);
        } catch (NullPointerException e) {
            log.error("Error thrown while writing the Dataset to HDFS." + e.getMessage());
        }
    }
}