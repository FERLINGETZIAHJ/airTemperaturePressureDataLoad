package com.weather;

import com.weather.common.CommonValidations;
import com.weather.temperature.AirTemperature;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

public class AirTemperatureTest {
    private static SparkSession spark;
    private static String directoryName = "C:\\Users\\FairyJacob\\Desktop\\Spark\\unitTest\\";

    private void initialSetup() {
        spark = SparkSession.builder().appName("MyAirTempDataAnalysisTestApp").config("spark.master", "local").getOrCreate();
        System.setProperty("hadoop.home.dir", "C:\\hadoop\\bin\\winutils.exe");
        Logger.getLogger("org").setLevel(Level.OFF);
    }
    private Dataset<org.apache.spark.sql.Row> createSourceDataset(String fileName)
    {
        Dataset<org.apache.spark.sql.Row> readFromSourceFile = AirTemperature.readInputAndFrameDataset(spark,directoryName,fileName);
        return readFromSourceFile;
    }

    /**
     * Validates the Air Temperature Data TextFile to check if it contains all the year data as in the FileName.
     */
    @Test
    public void testAirTemperatureFileData() {
        String fileName = "stockholmA_daily_temp_obs_2013_2017_t1t2t3txtntm.txt";
        initialSetup();
        Dataset<org.apache.spark.sql.Row> datasetReadFromFile = createSourceDataset(fileName);
        boolean validData = CommonValidations.validateIfFileContainsRelevantData(fileName,datasetReadFromFile);
        if(validData)
            System.out.println("Valid Records. File Contains all the Years of the Data");
        else
            System.out.println("File Records doesn't match with that of the File Name Years. Missing Data.");

        assert validData;
    }

    /**
     * Validates the Air Temperature Data by checking if it contains Duplicate Records.
     */
    @Test
    public void testAirTemperatureDSCount() {
        String fileName = "stockholmA_daily_temp_obs_2013_2017_t1t2t3txtntm.txt";
        initialSetup();
        Dataset<org.apache.spark.sql.Row> datasetReadFromFile = createSourceDataset(fileName);
        boolean duplicateExists = CommonValidations.duplicateExists(datasetReadFromFile);
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
    public void testAirTemperatureDSRank() {
        String fileName = "stockholmA_daily_temp_obs_2013_2017_t1t2t3txtntm.txt";
        initialSetup();
        Dataset<org.apache.spark.sql.Row> datasetReadFromFile = createSourceDataset(fileName);
        boolean isMultipleRecordsExistFlag = CommonValidations.checkForMultipleRecordsPerDay(spark,datasetReadFromFile.distinct());
        if(isMultipleRecordsExistFlag)
            System.out.println("There are Multiple Records for a Day. Please Check the Input Data.");

        assert !isMultipleRecordsExistFlag;
    }

    @Test
    public void testAirTemperatureSchema() {
        String fileName = "stockholmA_daily_temp_obs_2013_2017_t1t2t3txtntm.txt";
        initialSetup();
        Dataset<org.apache.spark.sql.Row> datasetReadFromFile = createSourceDataset(fileName);
        System.out.println(datasetReadFromFile.schema());
        Dataset<org.apache.spark.sql.Row> targetDS = AirTemperature.furtherDSProcessing(spark,datasetReadFromFile);

        boolean isSchemaMatching = CommonValidations.validateSchemaIntegrity(datasetReadFromFile.drop("_corrupt_record").drop("ISMANUAL"), targetDS.drop("PUBLISHED_CATEGORY"));

        if(isSchemaMatching)
            System.out.println("Schemas are same between Source and Target.");
        else
            System.out.println("Schema mismatches");

        assert isSchemaMatching;
    }
}
