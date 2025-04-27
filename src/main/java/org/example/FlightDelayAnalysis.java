package org.example;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

public class FlightDelayAnalysis {
    public static void main(String[] args){
        //initialize Spark session
        //SparkSession is the entry point to use Spark. Allows to create DataFrames, execute Sql queries, and perform distributed computing
        SparkSession spark = SparkSession.builder()
                .appName("Flight Delay Analysis")
                .master("local[*]") //it runs in local mode using all(*) available CPU cores
                .getOrCreate();  //creates a new Spark session or reuses an existing one

        // load the data set
        String filePath = args.length > 0 ? args[0] : "/Users/anedelcu/Lavinia_Nedelcu/School/archive/flights.csv"; //checks if at least one argument was provided
        Dataset<Row> flightData = spark.read()
                .option("header", "True") //1st row is header
                .option("inferSchema", "true") // detects the columns data types
                .csv(filePath);
        // Dataset<Row>: *
        //      *it is a distributed collection of data; is part of Spark structured API= essentially a DataFrame in Spark
        //      *You typically create a Dataset<Row> by reading data from a file (e.g., CSV, JSON) or from an existing collection. Data is loaded from csv to Dataset<Row>
        //      *You can perform SQL-like operations (e.g., select, filter, groupBy, agg) without writing complex code.
        //      *You can run SQL queries directly on a Dataset<Row>:

        //show the schema of the data set
        flightData.printSchema(); //schema = looks like a table with column names and data types

        // analysis
        // 1. The most delayed airlines
        Dataset<Row> mostDelayedAirlines = flightData.groupBy("AIRLINE")
                .agg(functions.sum("DEPARTURE_DELAY").alias("TotalDepartureDelay"),
                     functions.sum("ARRIVAL_DELAY").alias("TotalArrivalDelay") )
                .orderBy( functions.desc("TotalDepartureDelay") );
        System.out.println("Most Delayed Airlines");
        mostDelayedAirlines.show();
        
        // 2. Airports with Most Delays (departing & landing airport)
        Dataset<Row> mostDelayedOriginAirports = flightData.groupBy("ORIGIN_AIRPORT")
                .agg(functions.sum("DEPARTURE_DELAY").alias("TotalDepartureDelay"))
                .orderBy(functions.desc("TotalDepartureDelay"))
                .limit(10);
        System.out.println("Airports with Most Departure Delays:");
        mostDelayedOriginAirports.show();

        Dataset<Row> mostDelayedDestAirports = flightData.groupBy("DESTINATION_AIRPORT")
                .agg(functions.sum("ARRIVAL_DELAY").alias("TotalArrivalDelay"))
                .orderBy(functions.desc("TotalArrivalDelay"))
                .limit(10);
        System.out.println("Airports with Most Arrival Delays:");
        mostDelayedDestAirports.show();

        // 3. Average Delay by Day of Week (groups the data by day of the week)
        Dataset<Row> avgDelayByDayOfWeek = flightData.groupBy("DAY_OF_WEEK")
                .agg(functions.avg("DEPARTURE_DELAY").alias("AvgDepartureDelay"))
                .orderBy("DAY_OF_WEEK");
        System.out.println("Average Departure Delay by Day of Week:");
        avgDelayByDayOfWeek.show();

        // 4. Cancellation Reasons
        Dataset<Row> cancellationReasons = flightData.filter("CANCELLED = 1")
                .groupBy("CANCELLATION_REASON")
                .count()
                .orderBy(functions.desc("count"));
        System.out.println("Cancellation Reasons:");
        cancellationReasons.show();

        // WRITE the output to a csv file (this will write the output as a table, named most_..._airlines)
        // by default the output is just printed on the console. If you want the result to be written to a file, do the follow
        mostDelayedAirlines.coalesce(1).write()
                .mode("overwrite") //this will delete the existing folder before writing another one
                .option("header", "true")
                .csv("/Users/anedelcu/apache-spark/java-work/most_delayed_airlines");

        // Stop the SparkSession
        spark.stop();

    }
}
 //     * Spark’s DataFrame API does not natively support writing multiple tables into a single CSV file
 //     * You can do this by creating another class eg:writeToSingleCSV and use FileWriter write maybe...