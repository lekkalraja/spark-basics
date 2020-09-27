package com.spark.basics

import org.apache.spark.sql.functions.{approx_count_distinct, avg, col, count, countDistinct, max, mean, min, stddev, sum}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Aggregations extends App {

  private val spark: SparkSession = SparkSession.builder()
    .config("spark.master", "local")
    .appName("Aggregations")
    .getOrCreate()

  private val moviesDF: DataFrame = spark.read.option("inferSchema", "true").json("src/main/resources/data/movies.json")

  /**
   * Counting
   */

  /**
   *  Count's all the values expect nulls
   */
  moviesDF.select(count("Major_Genre"))//.show()
  moviesDF.selectExpr("count(Major_Genre)")//.show()

  /**
   * count's all the rows including null values
   */
  moviesDF.select(count("*"))//.show()

  /**
   * Counting Distinct Values
   */
  moviesDF.select(countDistinct("Major_Genre"))//.show() // Exclude null in the count
  //println(moviesDF.select("Major_Genre").distinct().count()) // Include null in the count

  /**
   * approximate count (will not scan through all the data set across the cluster)
   */

  moviesDF.select(approx_count_distinct(col("Major_Genre")))//.show()

  /**
   * Min and Max
   */
  moviesDF.select(min(col("IMDB_Rating")))//.show()
  moviesDF.selectExpr("min(IMDB_Rating)")//.show()
  moviesDF.select(max(col("IMDB_Rating")))//.show()
  moviesDF.selectExpr("max(IMDB_Rating)")//.show()

  /**
   * Sum
   */
  moviesDF.select(sum(col("US_Gross")))//.show()
  moviesDF.selectExpr("sum(US_Gross)")//.show()

  /**
   * Avg
   */
  moviesDF.select(avg(col("Rotten_Tomatoes_Rating")))//.show()
  moviesDF.selectExpr("avg(Rotten_Tomatoes_Rating)")//.show()

  /**
   * Mean & Stddev
   */
  moviesDF.select(
    mean(col("Rotten_Tomatoes_Rating")),
    stddev(col("Rotten_Tomatoes_Rating"))
  )//.show()

  /**
   * Grouping
   */

  moviesDF.groupBy("Major_Genre").count()//.show()
  moviesDF.groupBy("Major_Genre").avg("IMDB_Rating")//.show()

  moviesDF.groupBy("Major_Genre")
    .agg(
      count("*").as("N_Movies"),
      avg("IMDB_Rating").as("Avg_Rating"),
      sum("US_Gross").as("Total_US_Gross")
    )
    .orderBy("Total_US_Gross")
    //.show()

  /**
   * Exercises :
   *  1. Sum up All the profits of ALL the movies in the DF
   *  2. Count how many distinct directors we have
   *  3. Show the mean and standard deviation of US gross revenue for the movies
   *  4. Compute the average IMDB rating and the average US gross revenue PER Director
   */

  moviesDF.selectExpr("sum(US_Gross + Worldwide_Gross + US_DVD_Sales)").show()
  moviesDF.select(countDistinct(col("Director"))).show()
  moviesDF.select(mean(col("US_Gross")), stddev(col("US_Gross"))).show()
  moviesDF.groupBy("Director")
    .agg(
      avg(col("IMDB_Rating")),
      avg("US_Gross")
    )
    .show()
}