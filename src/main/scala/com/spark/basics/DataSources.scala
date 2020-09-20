package com.spark.basics

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, LongType, StringType, StructField, StructType}

object DataSources extends App {

  System.setProperty("hadoop.home.dir", "C:\\Users\\878127\\Documents\\Scala\\hadoop-2.8.1")

  private val spark: SparkSession = SparkSession.builder()
    .appName("Data Sources")
    .config("spark.master", "local")
    .getOrCreate()

  private val schema: StructType = StructType(
    Seq(
      StructField("Acceleration", DoubleType, nullable = true),
      StructField("Cylinders", LongType, nullable = true),
      StructField("Displacement", DoubleType, nullable = true),
      StructField("Horsepower", LongType, nullable = true),
      StructField("Miles_per_Gallon", DoubleType, nullable = true),
      StructField("Name", StringType, nullable = true),
      StructField("Origin", StringType, nullable = true),
      StructField("Weight_in_lbs", LongType, nullable = true),
      StructField("Year", DateType, nullable = true)
    )
  )
  /**
   * Following are required to load Data to DataFrames
   *  - format => json/csv/jdbc/orc/parquet
   *  - schema => enforce schema / infer schema
   *  - path => path of the source to load
   *  - zero or more options
   */

  private val dataFrame: DataFrame = spark.read
    .schema(schema)
    .format("json")
    //.option("dateFormat", "YYYY-MM-dd")
    .option("path", "src/main/resources/data/cars.json")
    .option("mode", "failFast") // dropMalformed, permissive (default)
    .load()

  spark.read
    //.format("json")
    .options(Map(
      "path" -> "src/main/resources/data/cars.json",
      "mode" -> "dropMalformed",
      "inferSchema" -> "true"
    ))
    .json()

  // NOTE : THERE ARE LOT OF WAYS TO GET DATAFRAME.. JUST OPEN AND EXPLORE

  /**
   * Writing Dataframes to external sources require
   *  - format => json/csv/parquet/orc/...
   *  - save mode => overwrite, append, ignore, errorIfExists
   *  - path
   *  - zero or more options
   */

  dataFrame.write
    .format("csv")
    .mode(SaveMode.Overwrite)
    .option("path", "src/main/resources/cars.csv")
    .save()
    //.csv("src/main/resources/data/cars.csv")

  //JSON Flags
  spark.read
    .schema(schema)
    .option("dateFormat", "YYYY-MM-dd") // couple with schema. if spark fails parsing, it will put null
    .option("allowSingleQuotes", "true")
    .option("compression", "uncompressed") //bzip2, gzip, lz4, snappy, deflate
    .json("src/main/resources/data/cars.json")
    .show()

  //CSV flags
  private val stocksSchema: StructType = StructType(
    Seq(
      StructField("symbol", StringType),
      StructField("date", DateType),
      StructField("price", DoubleType)
    )
  )
  spark.read
    .format("csv")
    .option("dateFormat", "MMM dd YYYY")
    .option("header", "true")
    .option("sep", ",")
    .option("nullValue", "")
    .option("path", "src/main/resources/data/stocks.csv")
    .load()
    .show()

  // Parquet
  dataFrame.write
    //.format("parquet") // Default format for reads/writes
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/cars.parquet") // snappy is the default compression

  //Text Files
  spark.read.text("src/main/resources/data/sample.txt").show()

  //JDBC
  spark.read
    .format("jdbc")
    .options(Map(
      "driver" -> "org.postgresql.Driver",
      "url" -> "jdbc:postgresql://localhost:5432/rtjvm",
      "user" -> "docker",
      "password" -> "docker",
      "dbtable" -> "public.employees"
    ))
    .load()
    .show()

  /**
   * Exercise : Read the movies DF, then write it as
   *  - tab-separated values files
   *  - snappy parquet
   *  - table "public.movies" in the Postgres DB
   */

  private val moviesDF: DataFrame = spark.read.format("json")
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  moviesDF.write
    .option("sep","\t")
    .option("header", "true")
    .mode(SaveMode.Overwrite)
    .csv("src/main/resources/movies.csv")

  moviesDF.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/movies.parquet")

  moviesDF.write
    .format("jdbc")
    .mode(SaveMode.Overwrite)
    .options(Map(
      "driver" -> "org.postgresql.Driver",
      "url" -> "jdbc:postgresql://localhost:5432/rtjvm",
      "user" -> "docker",
      "password" -> "docker",
      "dbtable" -> "public.movies"
    ))
    .save()

  spark.read
    .format("jdbc")
    .options(Map(
      "driver" -> "org.postgresql.Driver",
      "url" -> "jdbc:postgresql://localhost:5432/rtjvm",
      "user" -> "docker",
      "password" -> "docker",
      "dbtable" -> "public.movies"
    )).load()
    .show()
}