package com.spark.basics.typesdatasets

import org.apache.spark.sql.functions.{coalesce, col}
import org.apache.spark.sql.{DataFrame, SparkSession}

object ManagingNulls extends App {

  private val spark: SparkSession = SparkSession.builder()
    .appName("Managing Nulls")
    .config("spark.master", "local")
    .getOrCreate()

  private val movies: DataFrame = spark.read.json("src/main/resources/data/movies.json")

  /**
   * Select the first non-null value - (coalesce)
   */

  movies.select(
    col("Title"),
    col("Rotten_Tomatoes_Rating"),
    col("IMDB_Rating"),
    coalesce(col("Rotten_Tomatoes_Rating"),col("IMDB_Rating") * 10).as("Final_Rating")
  )
   // .show

  /**
   * Checking for nulls
   */
  movies.where(col("Rotten_Tomatoes_Rating").isNull)
    //.show

  /**
   * nulls when ordering
   */
  movies.orderBy(col("Rotten_Tomatoes_Rating").desc_nulls_last)
    //.show
  movies.orderBy(col("Rotten_Tomatoes_Rating").desc_nulls_first)
    //.show

  /**
   * Removing rows containing nulls (any one of the column)
   */
  movies.na.drop()
    //.show

  /**
   * Replacing nulls
   */
  movies.na.fill(0, List("Rotten_Tomatoes_Rating", "IMDB_Rating"))
   // .show

  movies.na.fill(Map(
    "Rotten_Tomatoes_Rating" -> 0,
    "IMDB_Rating" -> 0,
    "Director" -> "Unknown",
    "Creative_Type" -> "Unknown_Type",
    "Major_Genre" -> "Unknown"
  ))
    //.show

  /**
   * Complex Operations
   *  ifnull => same as coalesce
   *  nvl => same as coalesce and ifnull
   *  nullif => returns null if the two values are EQUAL, else first value
   *  nvl2 => if (first != null) second else third
   */

  movies.selectExpr(
    "Title",
    "Rotten_Tomatoes_Rating",
    "IMDB_Rating",
    "ifnull(Rotten_Tomatoes_Rating,IMDB_Rating * 10) as ifnull",
    "nvl(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as nvl",
    "nullif(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as nullif",
    "nvl2(Rotten_Tomatoes_Rating, IMDB_Rating * 10, 0.0) as nvl2"
  )
    //.show

}
