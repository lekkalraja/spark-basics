package com.spark.basics.dataframes

import org.apache.spark.sql.functions.{col, column, expr}
import org.apache.spark.sql._

object ColumnsAndExpressions extends App {

  System.setProperty("hadoop.home.dir", "C:\\Users\\878127\\Documents\\Scala\\hadoop-2.8.1")

  private val spark: SparkSession = SparkSession.builder()
    .appName("Columns And Expressions")
    .config("spark.master", "local")
    .getOrCreate()

  private val carsDF: DataFrame = spark.read.json("src/main/resources/data/cars.json")
  //carsDF.show()

  /**
   * Columns
   */
  private val acceleration: Column = carsDF.col("Acceleration")
  /**
   * Selecting (Projecting) only Acceleration Column in new DF
   */
  private val accelerationDF: DataFrame = carsDF.select(acceleration) // Will Create new DF with Acceleration Column alone
  //accelerationDF.show()

  /**
   * Various Select Methods
   */

  import spark.implicits._

  private val carsSubsetDF: DataFrame = carsDF.select(
    carsDF.col("Cylinders"),
    col("Displacement"), // from org.apache.spark.sql.functions.col
    column("Horsepower"), // from org.apache.spark.sql.functions.column
    'Miles_per_Gallon, //Scala Symbol, auto-converted to column using (spark implicits)
    $"Origin", // Fancier interpolated string, returns a Column Object
    expr("Weight_in_lbs") // EXPRESSION
  )
  //carsSubsetDF.show()

  /**
   * select with plain column names
   */
  private val plainColumns: DataFrame = carsDF.select("Name", "Year")
  //plainColumns.show()

  // NOTE : Selecting with Plain Names and Column objects should not mix

  /**
   * EXPRESSIONS
   */
  private val weightInLBS: Column = carsDF.col("Weight_in_lbs")
  private val weightInKGS: Column = carsDF.col("Weight_in_lbs") / 2.2

  private val carsWithWeightsDf: DataFrame = carsDF.select(col("Name"),
    weightInLBS,
    weightInKGS.as("Weight_in_kgs"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_kgs_2"))
  //carsWithWeightsDf.show()

  /**
   * selectExpr
   */
  private val carsWithSelectExprWeightsDF: DataFrame = carsDF.selectExpr("Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2")
  //carsWithSelectExprWeightsDF.show()

  /**
   * DataFrame Processing yiels new DF
   *  1. Adding a column
   *     2. Renaming a Column
   *     3. Removing a Column
   */

  private val carsDFWithKgs: DataFrame = carsDF.withColumn("Weight_in_kgs", col("Weight_in_lbs") / 2.2)
  //carsDFWithKgs.show()

  private val carsDFColumnRename: DataFrame = carsDFWithKgs.withColumnRenamed("Weight_in_lbs", "Weight in pounds")
  //carsDFColumnRename.show()
  //Care full with column Renames
  //carsDFColumnRename.selectExpr("`Weight in pounds`").show()

  private val carsDFWithLessColumns: DataFrame = carsDFColumnRename.drop("Cylinders", "Displacement")
  //carsDFWithLessColumns.show()

  /**
   * Filtering
   */
  private val carsWithoutUSAOrigin: Dataset[Row] = carsDF.filter(col("Origin") =!= "USA")
  private val carsWithUSAOrigin: Dataset[Row] = carsDF.where(col("Origin") === "USA")
  //carsWithoutUSAOrigin.show()
  //carsWithUSAOrigin.show()

  /**
   * Filtering with expression strings
   */
  private val americanCars: Dataset[Row] = carsDF.filter(" Origin = 'USA'")
  //americanCars.show()

  /**
   * Chain Filters
   */

  private val usaHighSpeedCars: Dataset[Row] = carsDF.filter(col("Origin") === "USA").filter(col("Horsepower") > 150)
  private val nonUSAHighSpeedCars: Dataset[Row] = carsDF.filter(col("Origin") =!= "USA").filter(col("Horsepower") > 150)
  private val usaMoreMiles: Dataset[Row] = carsDF.filter(" Origin = 'USA' and Miles_per_Gallon > 14")
  //usaHighSpeedCars.show()
  //nonUSAHighSpeedCars.show()
  //usaMoreMiles.show()

  /**
   * Unioning = adding more columns to DF
   */

  private val moreCarsDF: DataFrame = spark.read.option("inferSchema", "true").json("src/main/resources/data/more_cars.json")

  //moreCarsDF.show()

  private val allCars: Dataset[Row] = carsDF.union(moreCarsDF) //works if the DF's have the same Schema
  //println(s"Total Cars : ${carsDF.count()}")
  //println(s"All Cars : ${allCars.count()}")

  private val originatedCountries: Dataset[Row] = allCars.selectExpr("Origin").distinct()
  //originatedCountries.show()

  /**
   * Exercises
   *  1. Read the movies DF and select 2 columns of your choice
   *     2. Create another column summing up the total profit of the movies => (US_Gross + Worldwide_Gross + US_DVD_Sales)
   *     3. Select all Comedy movies with IMDB rating above 6
   */

  private val moviesDF: DataFrame = spark.read.option("inferSchema", "true").json("src/main/resources/data/movies.json")
  //moviesDF.show()
  /*moviesDF.select(col("Title"), col("Release_Date")).show()
  moviesDF.select("Title", "Release_Date").show()
  moviesDF.select(moviesDF.col("Title"), column("Release_Date")).show()
  moviesDF.selectExpr("Title", "Release_Date").show()*/

  private val moviesWithTotalProfits: DataFrame = moviesDF.withColumn("Total_Profits", col("US_Gross") + col("Worldwide_Gross"))
  //moviesWithTotalProfits.selectExpr("Title" , "US_Gross", "Worldwide_Gross",  "Total_Profits").show()
  //moviesDF.selectExpr("Title", "US_Gross", "Worldwide_Gross", "US_Gross + Worldwide_Gross as Total_Profits").show()
  //moviesDF.select(col("Title"), column("US_Gross"), $"Worldwide_Gross", (col("US_Gross") + col("Worldwide_Gross")).as("Total_Profit")).show()

  //moviesDF.where("Major_Genre = 'Comedy' and IMDB_Rating > 6" ).show()

}
