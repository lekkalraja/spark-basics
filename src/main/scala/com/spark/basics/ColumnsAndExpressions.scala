package com.spark.basics

import org.apache.spark.sql.functions.{col, column, expr}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object ColumnsAndExpressions extends App {

  System.setProperty("hadoop.home.dir", "C:\\Users\\878127\\Documents\\Scala\\hadoop-2.8.1")

  private val spark: SparkSession = SparkSession.builder()
    .appName("Columns And Expressions")
    .config("spark.master", "local")
    .getOrCreate()

  private val carsDF: DataFrame = spark.read.json("src/main/resources/data/cars.json")
  //carsDF.show()

  //Columns
  private val acceleration: Column = carsDF.col("Acceleration")
  //Selecting (Projecting) only Acceleration Column in new DF
  private val accelerationDF: DataFrame = carsDF.select(acceleration)// Will Create new DF with Acceleration Column alone
  //accelerationDF.show()

  // Various Select Methods
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

  // select with plain column names
  private val plainColumns: DataFrame = carsDF.select("Name", "Year")
  //plainColumns.show()

  // NOTE : Selecting with Plain Names and Column objects should not mix

  // EXPRESSIONS
  private val weightInLBS: Column = carsDF.col("Weight_in_lbs")
  private val weightInKGS: Column = carsDF.col("Weight_in_lbs") / 2.2

}