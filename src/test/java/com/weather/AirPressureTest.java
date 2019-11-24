package com.weather;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;
import scala.collection.Seq;
import org.apache.spark.sql.types.*;

import java.util.ArrayList;

import static org.apache.spark.sql.functions.col;

public class AirPressureTest {
    private static SparkSession spark;

    private void initialSetup() {
        spark = SparkSession.builder().appName("MyAirPressureDataAnalysisTestApp").config("spark.master", "local").getOrCreate();
        System.setProperty("hadoop.home.dir", "C:\\hadoop\\bin\\winutils.exe");
        Logger.getLogger("org").setLevel(Level.OFF);
    }
    private Dataset<Row> createSourceDataset(String fileName)
    {
        String rangeOfYears = AirPressure.rangeMapping(fileName);
        StructType customSchema = AirPressure.getCustomStructType(rangeOfYears);
        Dataset<org.apache.spark.sql.Row> dsReadFromSourceFile = AirPressure.read3TimesObsInputAndFrameDataset(spark, CommonConstants.unitTestdirectoryName, fileName, customSchema);
        return dsReadFromSourceFile;
    }

    private Dataset<Row> convertPressureDatatohPa(Dataset<Row> rawAirPressureDS, String fileName)
    {
        String rangeOfYears = AirPressure.rangeMapping(fileName);
        Seq<Column> selectColumns = AirPressure.getRequiredSelectColumnsSeq();
        Dataset<Row> airPressureDS = AirPressure.tohPaConversion(rawAirPressureDS, rangeOfYears, selectColumns);
        return airPressureDS;
    }
    /**
     * Validates the Air Pressure Data TextFile to check if it contains all the year data as in the FileName.
     */
    @Test
    public void testAirPressureFileData() {
        String fileName = "stockholm_barometer_1938_1960.txt";
        initialSetup();
        Dataset<org.apache.spark.sql.Row> datasetReadFromFile = createSourceDataset(fileName);
        Dataset<Row> convertedDS = convertPressureDatatohPa(datasetReadFromFile,fileName);
        boolean validData = CommonValidations.validateIfFileContainsRelevantData(fileName,convertedDS);
        if(validData)
            System.out.println("Valid Records. File Contains all the Years Data");
        else
            System.out.println("File Records doesn't match with that of the File Name Years. Missing Data.");

        assert validData;
    }

    /**
     * Validates the Air Temperature Data by checking if it contains Duplicate Records.
     */
    @Test
    public void testAirPressureDSCount() {
        String fileName = "stockholm_barometer_1938_1960.txt";
        initialSetup();
        Dataset<org.apache.spark.sql.Row> datasetReadFromFile = createSourceDataset(fileName);
        Dataset<Row> convertedDS = convertPressureDatatohPa(datasetReadFromFile,fileName);
        boolean duplicateExists = CommonValidations.duplicateExists(convertedDS);
        if(duplicateExists)
            System.out.println("Duplicate Records Exists. Proceeding by taking Distinct Records");
        else
            System.out.println("No Duplicate Records Found");

        assert !duplicateExists;

    }

    /**
     * Validates data by checking if it contains multiple records(not duplicate observations but different observations) for a day.
     */
    @Test
    public void testAirPressureDSRank() {
        String fileName = "stockholm_barometer_1938_1960.txt";
        initialSetup();
        Dataset<org.apache.spark.sql.Row> datasetReadFromFile = createSourceDataset(fileName);
        Dataset<Row> convertedDS = convertPressureDatatohPa(datasetReadFromFile,fileName);
        boolean isMultipleRecordsExistFlag = CommonValidations.checkForMultipleRecordsPerDay(spark,convertedDS.distinct());
        if(isMultipleRecordsExistFlag)
            System.out.println("There are Multiple Records for a Day. Please Check the Input Data.");

        assert !isMultipleRecordsExistFlag;
    }

    /**
     * Validate the Schema of the Source and Target Air Pressure Datasets.
     */
    @Test
    public void testAirPressureSchema() {
        String fileName = "stockholm_barometer_1859_1861.txt";
        //The Output Struct should be in this required format
        String requiredStructType = "stockholm_barometer_1938_1960.txt";
        initialSetup();
        Dataset<org.apache.spark.sql.Row> requiredSourceStruct = createSourceDataset(requiredStructType);
        Dataset<org.apache.spark.sql.Row> datasetReadFromFile = createSourceDataset(fileName);
        Dataset convertedDS = convertPressureDatatohPa(datasetReadFromFile,fileName);

        boolean isSchemaMatching = CommonValidations.validateSchemaIntegrity(requiredSourceStruct,convertedDS.drop("SPLIT_RANGE"));

        if(isSchemaMatching)
            System.out.println("Schemas are same between Source and Target.");
        else
            System.out.println("Schema mismatches");

        assert isSchemaMatching;
    }

    /**
     * This test validates the Conversion by following REVERSE CONVERSION TESTING.
     * ex: 1 hPa to mm Hg = 0.75006 mm Hg
     */
    @Test
    public void testAirPressureConversion() {
        boolean isConversionProper = false;
        Dataset convertedValueDS = null;
        String fileName = "stockholm_barometer_1859_1861.txt";
        ArrayList<String> conversionreq = new ArrayList<String>(3);
        conversionreq.add("1756_1858");
        conversionreq.add("1859_1861");
        conversionreq.add("1862_1937");
        initialSetup();
        Dataset<org.apache.spark.sql.Row> datasetReadFromFile = createSourceDataset(fileName);
        String rangeOfYears = AirPressure.rangeMapping(fileName);
        Dataset convertedDS = null;
        if(conversionreq.contains(rangeOfYears)) {
            convertedDS = convertPressureDatatohPa(datasetReadFromFile, fileName);
            Dataset processedDS = null;
            Dataset srcDS = null;
            if (rangeOfYears.equalsIgnoreCase("1862_1937")) {
                convertedValueDS = getConvertedDS(rangeOfYears, convertedDS, CommonConstants.hPaTommHg);
                processedDS = convertedValueDS.select("OBSERVATION1", "OBSERVATION2", "OBSERVATION3");
                srcDS = datasetReadFromFile.select("OBSERVATION1", "OBSERVATION2", "OBSERVATION3");
            } else if (rangeOfYears.equalsIgnoreCase("1756_1858")) {
                convertedValueDS = getConvertedDS(rangeOfYears, convertedDS, CommonConstants.hpaToSwedish);
                processedDS = convertedValueDS.na().fill(Double.NaN).select("OBSERVATION1", "OBSERVATION2", "OBSERVATION3");
                srcDS = datasetReadFromFile.select(col("BARO_OBS1").as("OBSERVATION1"), col("BARO_OBS2").as("OBSERVATION2"), col("BARO_OBS3").as("OBSERVATION3"));
            } else if (rangeOfYears.equalsIgnoreCase("1859_1861")) {
                convertedValueDS = getConvertedDS(rangeOfYears, convertedDS, CommonConstants.hpaToSwedishInchesPointOne);
                processedDS = convertedValueDS.select("OBSERVATION1", "OBSERVATION2", "OBSERVATION3");
                srcDS = datasetReadFromFile.select(col("REDUCED_AIR_PRESSURE1").as("OBSERVATION1"), col("REDUCED_AIR_PRESSURE2").as("OBSERVATION2"), col("REDUCED_AIR_PRESSURE3").as("OBSERVATION3"));
            }
            if (processedDS.except(srcDS).count() == 0)
                isConversionProper = true;
            else
                isConversionProper = false;
            assert isConversionProper;
        }else {
            System.out.println("No Conversion of Unit is required");
            assert !isConversionProper;
        }
    }
    public Dataset getConvertedDS(String rangeOfYears,Dataset convertedDS,Double unit)
    {
        if(rangeOfYears.equalsIgnoreCase("1756_1858")) {
            return convertedDS
                    .withColumn("OBSERVATION1", col("OBSERVATION1").multiply(unit).cast(new DecimalType(38, 2)))
                    .withColumn("OBSERVATION2", col("OBSERVATION2").multiply(unit).cast(new DecimalType(38, 2)))
                    .withColumn("OBSERVATION3", col("OBSERVATION3").multiply(unit).cast(new DecimalType(38, 2)));
        }else {
            return convertedDS
                    .withColumn("OBSERVATION1", col("OBSERVATION1").multiply(unit).cast(new DecimalType(38, 1)))
                    .withColumn("OBSERVATION2", col("OBSERVATION2").multiply(unit).cast(new DecimalType(38, 1)))
                    .withColumn("OBSERVATION3", col("OBSERVATION3").multiply(unit).cast(new DecimalType(38, 1)));
        }

    }

    /**
     * Validates if the Air Pressure Range is within the hPa Valid Range recorded by SMHI.
     * Highest air pressure: 23 January 1907 in Kalmar and Visby 1063.7 hPa.[90]
     * Lowest air pressure: 6 December 1895 Härnösand 938.4 hPa.[90]
     */
    @Test
    public void testValidAirPressureRange() {
        double minAirPressure = 938.4;
        double maxAirPressure = 1063.7;
        boolean isvalidRange = false;
        String fileName = "stockholm_barometer_1938_1960.txt";
        initialSetup();
        Dataset<org.apache.spark.sql.Row> datasetReadFromFile = createSourceDataset(fileName);
        Dataset convertedDS = convertPressureDatatohPa(datasetReadFromFile,fileName);
        Dataset filteredConvertedDS = convertedDS.filter(convertedDS.col("OBSERVATION1").between(minAirPressure,maxAirPressure))
        .filter(convertedDS.col("OBSERVATION2").between(minAirPressure,maxAirPressure))
        .filter(convertedDS.col("OBSERVATION3").between(minAirPressure,maxAirPressure));

        if(!(convertedDS.count()==filteredConvertedDS.count()))
            isvalidRange = true;
        else
            System.out.println("Air Pressure is Lower/Higher than that of the Recorded Ones");

        assert isvalidRange;
    }

}
