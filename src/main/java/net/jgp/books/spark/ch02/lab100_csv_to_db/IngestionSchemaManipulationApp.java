package net.jgp.books.spark.ch02.lab100_csv_to_db;

import org.apache.spark.Partition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class IngestionSchemaManipulationApp {

    public static void main(String[] args) {
        IngestionSchemaManipulationApp app =
                new IngestionSchemaManipulationApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("Restaurants in Wake County, NC")
                .master("local")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .format("csv")
                .option("header", "true")
                .load("data/Restaurants_in_Wake_County_NC.csv");
        System.out.println("*** Right after ingestion");

        System.out.println("*** Looking at partitions");
        Partition[] partitions = df.rdd().partitions();
        int partitionCount = partitions.length;
        System.out.println("Partition count before repartition: " +
                partitionCount);

        df.show(5);

    }
}
