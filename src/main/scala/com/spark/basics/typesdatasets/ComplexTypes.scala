package com.spark.basics.typesdatasets

import org.apache.spark.sql.functions.{array_contains, col, current_date, current_timestamp, datediff, expr, split, struct, to_date, when}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

object ComplexTypes extends App {

  private val spark: SparkSession = SparkSession.builder()
    .appName("Spark Complex Types")
    .config("spark.master", "local")
    .getOrCreate()

  private val movies: DataFrame = spark.read.json("src/main/resources/data/movies.json")

  /**
   * Dates
   */
  movies.select(
    col("Title"),
    to_date(col("Release_Date"), "d-MMM-yy"). as("Actual_Release_Date"),
  )
    .withColumn("Today", current_date())
    .withColumn("Right_Now", current_timestamp())
    .withColumn("Movie_Age", datediff(col("Today"), col("Actual_Release_Date")))
    //.show

  /**
   * Exercise
   */

  /**
   *  1. How to deal with multiple date formats ?
   */
  movies.select(
    col("Title"),
  when(to_date(col("Release_Date"), "d-MMM-yy").isNotNull,to_date(col("Release_Date"), "d-MMM-yy"))
    .when(to_date(col("Release_Date"), "yyyy-MM-dd").isNotNull,to_date(col("Release_Date"), "yyyy-MM-dd"))
    .when(to_date(col("Release_Date"), "MMM, dd, yyyy").isNotNull,to_date(col("Release_Date"), "MMM, dd, yyyy"))
    .otherwise("Unknown_Format")
    .as("Actual_Release_Date")
  )
    .where(col("Actual_Release_Date") equalTo "Unknown_Format")
    //.show

  /**
   *  2. Read the stocks DF and parse the dates
   */

  private val stocks: DataFrame = spark.read.option("header", "true").csv("src/main/resources/data/stocks.csv")
  stocks.select(
    col("symbol"),
    to_date(col("date"), "MMM d yyyy").as("Date"),
    col("price")
  )
    //.show

  /**
   * Structures / structs
   */

  /**
   * 1. With col operators
   */
  movies.select(
    col("Title"),
    struct(col("US_Gross"), col("Worldwide_Gross")).as("Profit")
  )
    .select(col("Title"), col("Profit").getField("US_Gross").as("US_Profit"))
   // .show

  /**
   * 2. With Expression strings
   */
  movies
    .selectExpr("Title", "(US_Gross, Worldwide_Gross) as Profit")
    .selectExpr("Title", "Profit.US_Gross as US_Profit")
    //.show

  /**
   * Arrays
   */

  movies
    .select(
      col("Title"),
      split(col("Title"), " |,").as("Title_Words")
    )
    .select(
      col("Title"),
      expr("Title_Words[0]"),
      functions.size(col("Title_Words")),
      array_contains(col("Title_Words"), "Love")
    )
    .show
}