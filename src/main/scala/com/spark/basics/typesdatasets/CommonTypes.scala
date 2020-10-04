package com.spark.basics.typesdatasets

import org.apache.spark.sql.functions.{col, initcap, lit, not, regexp_extract, regexp_replace}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object CommonTypes extends App {

  private val spark: SparkSession = SparkSession.builder()
    .appName("Common Spark Types")
    .config("spark.master", "local")
    .getOrCreate()

  private val movies: DataFrame = spark.read.json("src/main/resources/data/movies.json")

  /**
   * Adding a Plain value to Dataframe
   */
  movies.select(col("Title"), lit("US_PRODUCED").as("Producer"))
    //.show()

  /**
   * Booleans
   *  - Multiple Ways of Filtering
   */
  movies.where(col("Major_Genre") equalTo "Drama")
    .filter(col("Production_Budget") > 18000000)
    //.show() // Chained filters

  private val dramaFilter: Column = col("Major_Genre") equalTo "Drama"
  private val highBudgetFilter: Column = col("Production_Budget") > 18000000
  private val highBudgetDramaFilter: Column = dramaFilter and highBudgetFilter
  movies.where(highBudgetDramaFilter)
    //.show()

  movies.select(col("Title"), highBudgetDramaFilter.as("High_Budget_Drama"))
    .where("High_Budget_Drama")
    .drop("High_Budget_Drama")
    //.show()

  /**
   * Negations
   */
  movies.select(col("Title"), highBudgetDramaFilter.as("High_Budget_Drama"))
    .where(not(col("High_Budget_Drama")))
    .drop("High_Budget_Drama")
    //.show()

  /**
   * Numbers
   */
  movies.select(col("Title"), (((col("Rotten_Tomatoes_Rating") / 10) + col("IMDB_Rating"))/2).as("Avg_Rating"))
    .where(col("Avg_Rating") > 7.8)
    //.show()

  /**
   * Correlation  = number between -1 and 1
   */
  println(movies.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating"))

  /**
   * String Operations
   */
  private val cars: DataFrame = spark.read.json("src/main/resources/data/cars.json")

  /**
   * Capitalization - initcap, lower, upper
   */
  cars.select(initcap(col("Name")))
    //.show()

  /**
   * Contains
   */
  cars.where(col("Name").contains("volkswagen") or col("Name").contains("vw"))
    //.show()

  /**
   * Regex
   */
  cars.select(
    col("Name"),
    regexp_extract(col("Name"), "volkswagen|vw", 0).as("reg_exp")
  )
    .filter(col("reg_exp") =!= "")
    //.show()

  cars.select(
    col("Name"),
    regexp_replace(col("Name"), "volkswagen|vw", "People's Car").as("Renamed"),
    regexp_extract(col("Name"), "volkswagen|vw", 0).as("reg_exp")
  )
    .drop("Name")
    .where(col("reg_exp") notEqual "")
    .drop("reg_exp")
   // .show()

  /**
   * Exercise
   *  - Filter  the cars DF by a list of car names obtained by an API call
   *    Versions :
   *      - contains
   *      - regexes
   */

  /**
   * Contains version
   */
  private var filterByCarNames: Column = col("Name") contains getCarNames.head.toLowerCase
  getCarNames.tail.map(_.toLowerCase).foreach { car =>
    filterByCarNames = filterByCarNames or (col("Name") contains car )
  }

  private val filterByCarNames1: Column = getCarNames.map(_.toLowerCase)
    .map(car => col("Name").contains(car))
    .fold(lit(false))((combinedFilter, newFilter) => combinedFilter or newFilter)

  cars.where(filterByCarNames1)
    //.show()

  /**
   * Regex version
   */
  var filterByCarNamesRegex = getCarNames.head
  getCarNames.tail.foreach { car =>
    filterByCarNamesRegex = filterByCarNamesRegex.concat("|").concat(car)
  }

  var filterByCarNamesRegex1 = getCarNames.mkString("|")

  cars.select(
    col("*"),
    regexp_extract(col("Name"), filterByCarNamesRegex1, 0).as("reg_exp")
  )
    .where(col("reg_exp") =!= "")
    .drop("reg_exp")
   // .show()



  /**
   * Assume that this method is consuming an external endpoint to get the list of cars
   * @return
   */
  def getCarNames: List[String] = List("audi", "ford", "toyota")
}
