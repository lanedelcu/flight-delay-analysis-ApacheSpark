package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
public class AlaskaAirlinesAnalysis {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Alaska Airlines Delay Analysis")
                .master("local[*]")
                .getOrCreate();

        // Load dataset
        String filePath = args.length > 0 ? args[0] : "/Users/anedelcu/Lavinia_Nedelcu/School/archive/flight_data.csv";
        Dataset<Row> flightData = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(filePath);

        // Filter for Alaska Airlines (AS) flights only
        Dataset<Row> alaskaFlights = flightData.filter("AIRLINE = 'AS'");

        // Compute statistics for arrival delay
        Dataset<Row> alaskaDelayStats = alaskaFlights.agg(
                functions.min("ARRIVAL_DELAY").alias("MinArrivalDelay"),
                functions.max("ARRIVAL_DELAY").alias("MaxArrivalDelay"),
                functions.avg("ARRIVAL_DELAY").alias("AvgArrivalDelay")
        );

        System.out.println("Arrival Delay Statistics for Alaska Airlines:");
        alaskaDelayStats.show();

        // Display sample rows for verification
        System.out.println("Sample Alaska Airlines Flights:");
        alaskaFlights.select("FLIGHT_NUMBER", "ORIGIN_AIRPORT", "DESTINATION_AIRPORT", "ARRIVAL_DELAY").show(10);

        // Define output directory for saving results
        String outputDir = "/Users/anedelcu/apache-spark/java-work/alaska_airlines_analysis/alaska_delay_stats";

        // Save the Alaska Airlines delay statistics as a CSV file
        alaskaDelayStats.coalesce(1).write()
                .mode("overwrite")
                .option("header", "true")
                .csv(outputDir);
    }
}