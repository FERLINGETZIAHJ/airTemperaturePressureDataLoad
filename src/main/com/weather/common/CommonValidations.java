package com.weather.common;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;

import static org.apache.spark.sql.functions.*;

public class CommonValidations {

    private static final Logger log = Logger.getLogger(CommonValidations.class.getName());
    /**
     * This method is to validate if duplicate records exists.
     * @param airtemperaturePressureDS - "Dataset containing all the Merged Data from TextFile"
     * @return boolean
     */
    public static boolean duplicateExists(Dataset<Row> airtemperaturePressureDS) {
        if (airtemperaturePressureDS.count() == airtemperaturePressureDS.distinct().count()) {
            log.info("No Duplicate Records found");
            return false;
        } else {
            log.info("Duplicate Records found. Taking only distinct Records.");
            return true;
        }
    }

    /**
     * This Method validates if the File Contains relevvant Data as mentioned in the File Name.
     * @param fileName - Name of the Input File.
     * @param sourceDS - The Input Dataset which will be generated based on the File.
     * @return boolean
     */
    public static boolean validateIfFileContainsRelevantData(String fileName, Dataset sourceDS)
    {
        boolean validData = false;
        int startYear,endYear;
        String[] getYears = fileName.split("_");
        if(getYears.length > 5) {
            startYear = Integer.parseInt(getYears[4]);
            endYear = Integer.parseInt(getYears[5]);
        }else {
            startYear = Integer.parseInt(getYears[2]);
            endYear = Integer.parseInt(getYears[3].substring(0,4));
        }
        long totalYears = (endYear-startYear)+1;
        Dataset<FieldNames> yearDS = sourceDS
                .select(min("YEAR").alias("startYear"),max("YEAR").alias("endYear"),countDistinct("YEAR").alias("yearCount"))
                .as(Encoders.bean(FieldNames.class));

        int startYearFromDS = yearDS.first().getStartYear();
        int endYearFromDS = yearDS.first().getEndYear();
        long totalYearsFromDS = yearDS.first().getYearCount();
        log.info("startYear -"+startYear+ " endYear -"+endYear+ "startYearFromDS -"+startYearFromDS+ " endYearFromDS -"+endYearFromDS);
        if(startYear == startYearFromDS && endYear == endYearFromDS && totalYears == totalYearsFromDS)
            validData = true;
        return validData;
    }

    /**
     * This method is to checks if multiple records exists for the same day.
     * @param spark            - "Spark Session"
     * @param weatherDS - "Distinct Record Dataset"
     */
    public static boolean checkForMultipleRecordsPerDay(SparkSession spark, Dataset<Row> weatherDS) {
        boolean multipleRecordsPerDayExists = false;
        try {
            Dataset<org.apache.spark.sql.Row> checkForSameDateDuplicateEntries = weatherDS
                    .withColumn("MultipleRecordsCheck", dense_rank().over(Window.orderBy(col("MONTH"), col("DAY"), col("YEAR"))));
            checkForSameDateDuplicateEntries.createOrReplaceTempView("checkForSameDateDuplicateEntries");
            long weatherDSCount = weatherDS.count();
            long distinctRank = spark.sql("select DISTINCT(MultipleRecordsCheck) from checkForSameDateDuplicateEntries").count();

            if (weatherDSCount == distinctRank) {
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
     * This is to validate the Schema Consistency throughout the Data Processing Flow.
     * @param sourceDS - "Initial Dataset"
     * @param targetDS - "Final Dataset"
     */
    public static boolean validateSchemaIntegrity(Dataset sourceDS, Dataset targetDS){
        String srcSchema = sourceDS.schema().toString();
        String targetSchema = targetDS.schema().toString();
        //ignoring the nullable column values.
        return srcSchema.replace("true", "false").equalsIgnoreCase(targetSchema.replace("true", "false"));
    }

}
