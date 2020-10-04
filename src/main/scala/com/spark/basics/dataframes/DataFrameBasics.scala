package com.spark.basics.dataframes

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

object DataFrameBasics extends App {

  // Get Spark Session to interact With Dataframes
  private val spark: SparkSession = SparkSession.builder()
    .appName("Dataframe Basics")
    .config("spark.master", "local")
    .getOrCreate()

  private val carsFrame: DataFrame = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  /* carsFrame.show() // print 20 (by default) s as table
  carsFrame.printSchema() // By Default infer schema from data at runtime
  carsFrame.take(5).foreach(println) // Will print as tuples instead of table
  println(carsFrame.schema)*/

  // Creating Schema Manually
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
      StructField("Year", StringType, nullable = true)
    )
  )

  // Creating Dataframe with Schema by specifying upfront
  private val carsFrameWithSchema: DataFrame = spark.read.schema(schema).json("src/main/resources/data/cars.json")
  /*println(carsFrameWithSchema.schema)
  carsFrameWithSchema.show()
  carsFrameWithSchema.printSchema()
  carsFrameWithSchema.take(10).foreach(println)*/

  val cars = Seq(
    ("chevrolet chevelle malibu", 18.0, 8L, 307.0, 130L, 3504L, 12.0, "1970-01-01", "USA"),
    ("buick skylark 320", 15.0, 8L, 350.0, 165L, 3693L, 11.5, "1970-01-01", "USA"),
    ("plymouth satellite", 18.0, 8L, 318.0, 150L, 3436L, 11.0, "1970-01-01", "USA"),
    ("amc rebel sst", 16.0, 8L, 304.0, 150L, 3433L, 12.0, "1970-01-01", "USA"),
    ("ford torino", 17.0, 8L, 302.0, 140L, 3449L, 10.5, "1970-01-01", "USA"),
    ("ford galaxie 500", 15.0, 8L, 429.0, 198L, 4341L, 10.0, "1970-01-01", "USA"),
    ("chevrolet impala", 14.0, 8L, 454.0, 220L, 4354L, 9.0, "1970-01-01", "USA"),
    ("plymouth fury iii", 14.0, 8L, 440.0, 215L, 4312L, 8.5, "1970-01-01", "USA"),
    ("pontiac catalina", 14.0, 8L, 455.0, 225L, 4425L, 10.0, "1970-01-01", "USA"),
    ("amc ambassador dpl", 15.0, 8L, 390.0, 190L, 3850L, 8.5, "1970-01-01", "USA")
  )

  private val manualCarsFrame: DataFrame = spark.createDataFrame(cars) //schema auto-inferred and column names will be like _1, _2, ...
  /*println(manualCarsFrame.schema)
  manualCarsFrame.show()
  manualCarsFrame.printSchema()
  manualCarsFrame.take(10).foreach(println)*/

  // NOTE : DF'S HAVE SCHEMA, NOT ROWS

  //Create DF'S with implicits

  import spark.implicits._

  private val fromSeqWithoutColumns: DataFrame = cars.toDF()
  /*println(fromSeqWithoutColumns.schema)
  fromSeqWithoutColumns.show()
  fromSeqWithoutColumns.printSchema()
  fromSeqWithoutColumns.take(10).foreach(println)*/

  private val fromSeqWIthColumns: DataFrame = cars.toDF("Name", "Miles_per_Gallon", "Cylinders", "Displacement", "Horsepower", "Weight_in_lbs", "Acceleration", "Year", "Origin")
  /* println(fromSeqWIthColumns.schema)
  fromSeqWIthColumns.show()
  fromSeqWIthColumns.printSchema()
  fromSeqWIthColumns.take(10).foreach(println)*/

  /**
   * Exercise :
   *  1. Create a manual DF Describing smartphones
   *      - Owner
   *      - model
   *      - screen dim
   *      - camera megapixels
   *      - ram
   */

  private val smartPhones = Seq(
    ("Apple", "IPhone-X", "1024x678", "12", "8GB"),
    ("Nokia", "11", "1020x543", "11", "4GB"),
    ("OPPO", "11", "1020x543", "11", "4GB"),
    ("SAMSUNG", "11", "1020x543", "11", "4GB"),
    ("SONY", "11", "1020x543", "11", "4GB"),
  )

  private val smartPhonesDF: DataFrame = smartPhones.toDF("Owner", "model", "screen_dim", "camera_megapixels", "ram")
  /* println(smartPhonesDF.schema)
  smartPhonesDF.printSchema()
  smartPhonesDF.show()*/

  /**
   * Exercise :
   * 2) Read another json file from the data folder (movies.json)
   *      - print it's schema
   *      - count the number of rows
   */

  private val moviesFrame: DataFrame = spark.read.json("src/main/resources/data/movies.json")
  println(moviesFrame.schema)
  moviesFrame.printSchema()
  println(moviesFrame.count()) // Total number of rows in the frame

}
