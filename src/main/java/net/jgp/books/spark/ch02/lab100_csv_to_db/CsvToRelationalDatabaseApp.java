package net.jgp.books.spark.ch02.lab100_csv_to_db;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * CSV to a relational database.
 * 
 * @author jgp
 */
public class CsvToRelationalDatabaseApp {

  private static final String MYSQL_URL = "jdbc:mysql://localhost:3306/movies?characterEncoding=utf8&useSSL=false&serverTimezone=Asia/Shanghai&rewriteBatchedStatements=true";

  private static final String MYSQL_USERNAME = "root";

  private static final String MYSQL_PASSWORD = "123456";

  private static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";

  /**
   * main() is your entry point to the application.
   * 
   * @param args
   */
  public static void main(String[] args) {
    CsvToRelationalDatabaseApp app = new CsvToRelationalDatabaseApp();
    app.start();
  }

  /**
   * The processing code.
   */
  private void start() {
    // Creates a session on a local master
    SparkSession spark = SparkSession.builder()
        .appName("CSV to DB")
        .master("local")
        .getOrCreate();

    // Step 1: Ingestion
    // ---------

    // Reads a CSV file with header, called authors.csv, stores it in a
    // dataframe
    Dataset<Row> df = spark.read()
        .format("csv")
        .option("header", "true")
        .load("data/authors.csv");

    // Step 2: Transform
    // ---------

    // Creates a new column called "name" as the concatenation of lname, a
    // virtual column containing ", " and the fname column
    df = df.withColumn(
        "name",
        concat(df.col("lname"), lit(", "), df.col("fname")));

    // Step 3: Save
    // ---------

    // The connection URL, assuming your PostgreSQL instance runs locally on
    // the
    // default port, and the database we use is "spark_labs"
    String dbConnectionUrl = "jdbc:postgresql://localhost/spark_labs";

    // Properties to connect to the database, the JDBC driver is part of our
    // pom.xml
    Properties prop = new Properties();
    prop.setProperty("driver", MYSQL_DRIVER);
    prop.setProperty("user", MYSQL_USERNAME);
    prop.setProperty("password", MYSQL_PASSWORD);

    // Write in a table called ch02
    df.write()
        .mode(SaveMode.Overwrite)
        .jdbc(MYSQL_URL, "authors", prop);

    System.out.println("Process complete");
  }
}
