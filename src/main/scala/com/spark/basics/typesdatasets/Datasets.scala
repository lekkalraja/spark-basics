package com.spark.basics.typesdatasets

import org.apache.spark.sql._
import org.apache.spark.sql.functions.array_contains

/**
 *
 ******************* type DataFrame = Dataset[Row]************************
 * Typed DataFrames : Distributed collection of JVM objects
 *  Most Useful when :
 *    - we want to maintain type information
 *    - we want clean concise code
 *    - our filters/transformations are hard to express in DF or SQL
 *
 *  Avoid when :
 *    - performance is critical : Spark can't optimize transformations
 */
object Datasets extends App {

  private val spark: SparkSession = SparkSession.builder()
    .appName("Datasets")
    .config("spark.master", "local")
    .getOrCreate()

  private val numbersDF: DataFrame = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/main/resources/data/numbers.csv")

  //numbersDF.printSchema

  /**
   * Converting Dataframe to Dataset
   */
  implicit private val intEncoder: Encoder[Int] = Encoders.scalaInt
  private val numbersDS: Dataset[Int] = numbersDF.as[Int]
  numbersDS.filter(_ < 100)
    //.show

  /**
   * Converting Complex Dataframe to Dataset
   */

  // Dataset of a Complex Type
  case class Car(
                Name: String,
                Miles_per_Gallon: Option[Double], // Option will accept null values too
                Cylinders: Option[Long],
                Displacement: Option[Double],
                Horsepower: Option[Long],
                Weight_in_lbs: Option[Long],
                Acceleration: Option[Double],
                Year: Option[String],
                Origin: Option[String]
                )

  def readDF(fileName: String) : DataFrame = spark.read.option("inferSchema", "true").json(s"src/main/resources/data/$fileName")

  // Get Dataframe
  private val carsDF: DataFrame = readDF("cars.json")

  // Create Complex Type Encoder
  //implicit private val carsEncoder: Encoder[Car] = Encoders.product[Car]
  import spark.implicits._

  // Convert Dataframe to Dataset
  private val carsDS: Dataset[Car] = carsDF.as[Car]

  private val carNamesDS: Dataset[String] = carsDS.map(car => car.Name.toUpperCase)
  //carNamesDS.show

  /**
   * Exercises
   */

  /**
   * 1. count how many cars we have (406)
   */
  private val carsCount: Long = carsDS.count()
  //println(s"Cars Count $carsCount")

  /**
   * 2. Count how many Powerful (HP > 10) cars we have (81)
   */
  private val powerfulHP: Long = carsDS.filter(_.Horsepower.getOrElse(0L) > 140).count()
  //println(powerfulHP)

  /**
   * 3. Average HP for the entire dataset (103)
   */

  private val hpSUM: Long = carsDS.map(_.Horsepower.getOrElse(0L)).reduce(_ + _)
  //println($"HP AVG ${hpSUM/carsCount}")
  carsDS.selectExpr("avg(Horsepower)")
    //.show

  /**
   * Joins
   */

  case class Bands(id: Long, name: String, hometown: String, year: Long)
  case class Guitars(id: Long, model: String, make: String, guitarType: String)
  case class GuitarPlayers(id: Long, name: String, guitars: Seq[Long], band: Long)

  private val bandsDS: Dataset[Bands] = readDF("bands.json").as[Bands]
  private val guitarsDS: Dataset[Guitars] = readDF("guitars.json").as[Guitars]
  private val guitarPlayersDS: Dataset[GuitarPlayers] = readDF("guitarPlayers.json").as[GuitarPlayers]

  private val guitarPlayersWithBandsDS: Dataset[(GuitarPlayers, Bands)] = guitarPlayersDS.joinWith(bandsDS, guitarPlayersDS.col("band") equalTo bandsDS.col("id"), "inner")

  guitarPlayersWithBandsDS
    .withColumnRenamed("_1", "GuitarPlayers")
    .withColumnRenamed("_2", "Bands")
    //.show

  /**
   * Exercise : Join the guitarDS and guitarPlayersDS, in an outer join
   */

  private val guitarsWithPlayersDS: Dataset[(Guitars, GuitarPlayers)] = guitarsDS.joinWith(guitarPlayersDS,
    array_contains(guitarPlayersDS.col("guitars"), guitarsDS.col("id")),
    "left_outer")

  guitarsWithPlayersDS
    .withColumnRenamed("_1", "Guitars")
    .withColumnRenamed("_2", "GuitarPlayers")
   // .show


  /**
   * Aggregations
   */
  private val carsOriginWithCount: Dataset[(Option[String], Long)] = carsDS.groupByKey(_.Origin).count()
  carsOriginWithCount.show
}
