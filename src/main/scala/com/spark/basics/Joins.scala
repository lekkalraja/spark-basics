package com.spark.basics

import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.{DataFrame, SparkSession}

object Joins extends App {

  private val spark: SparkSession = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  private val bandsDF: DataFrame = spark.read.option("inferSchema", "true").json("src/main/resources/data/bands.json")
  private val guitarPlayersDF: DataFrame = spark.read.option("inferSchema", "true").json("src/main/resources/data/guitarPlayers.json")
  private val guitarsDF: DataFrame = spark.read.option("inferSchema", "true").json("src/main/resources/data/guitars.json")

  /**
   * JOINS
   */
  guitarPlayersDF.join(bandsDF, guitarPlayersDF.col("band") === bandsDF.col("id"), "inner")//.show()

  /**
   * OUTER JOINS
   */
  guitarPlayersDF.join(bandsDF, guitarPlayersDF.col("band") === bandsDF.col("id"), "left_outer")//.show()
  guitarPlayersDF.join(bandsDF, guitarPlayersDF.col("band") === bandsDF.col("id"), "right_outer")//.show()
  guitarPlayersDF.join(bandsDF, guitarPlayersDF.col("band") === bandsDF.col("id"), "outer")//.show()

  /**
   * SEMI-JOINS
   */
  /**
   * semi-joins : everything in the left DF for which there is a row in the right DF satisfying the condition
   */
  guitarPlayersDF.join(bandsDF, guitarPlayersDF.col("band") === bandsDF.col("id"), "left_semi")//.show()

  /**
   * anti-joins = Everything in the left DF for which there is NO row in the right DF satisfying the condition
   */
  guitarPlayersDF.join(bandsDF, guitarPlayersDF.col("band") === bandsDF.col("id"), "left_anti")//.show()

  // Things to bear in mind
  guitarPlayersDF.join(bandsDF, guitarPlayersDF.col("band") === bandsDF.col("id"), "inner")//.select("id", "band")//.show() // It will crash due to => Reference 'id' is ambiguous

  /**
   * Option 1 : Rename the column on which we are joining
   */
  guitarPlayersDF.join(bandsDF.withColumnRenamed("id", "band"), "band").select("id", "band")//.show()

  /**
   * Option 2 : Drop the duplicate column from the resultant set
   */
  guitarPlayersDF.join(bandsDF, guitarPlayersDF.col("band") === bandsDF.col("id"), "inner")
    .drop(bandsDF.col("id"))
    .select("id", "band")
    //.show()

  /**
   * Using Complex Types
   */

  guitarPlayersDF.join(guitarsDF.withColumnRenamed("id", "guitarId"), expr("array_contains(guitars, guitarId)"))//.show()

}