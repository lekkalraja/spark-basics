package com.spark.basics

import com.spark.basics.DataFrameBasics.spark
import org.apache.spark.sql.functions.{col, expr, max}
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

  /**
   * Exercises
   */

  private val postgres = Map(
    "driver" -> "org.postgresql.Driver",
    "url" -> "jdbc:postgresql://localhost:5432/rtjvm",
    "user" -> "docker",
    "password" -> "docker",
  )

  private val employeesDF: DataFrame = spark.read.format("jdbc").options(postgres + ("dbtable" -> "public.employees")).load()
  private val salariesDF: DataFrame = spark.read.format("jdbc").options(postgres + ("dbtable" -> "public.salaries")).load()
  private val deptMgrDF: DataFrame = spark.read.format("jdbc").options(postgres + ("dbtable" -> "public.dept_manager")).load()
  private val titlesDF: DataFrame = spark.read.format("jdbc").options(postgres + ("dbtable" -> "public.titles")).load()

  /**
   * 1. Show all employees and their max salary
   */
  salariesDF.groupBy(salariesDF.col("emp_no"))
    .max("salary").as("max_salary")
    .join(employeesDF, "emp_no")
//    .show()

  /**
   * 2. Show all employees who were never managers
   */
  titlesDF.where(" title = 'Manager'")
    .join(employeesDF, Seq("emp_no"), "right_outer")
    //.show()
  employeesDF.join(deptMgrDF, Seq("emp_no"), "left_anti")
    //.show()

  import spark.implicits._
  /**
   *  3. find the job titles of the best paid 10 employees in the company
   */
  salariesDF.groupBy("emp_no")
    .max("salary").as("max_salary")
    .limit(10)
    .join(employeesDF, "emp_no")
    //.join(titlesDF.groupBy("emp_no").agg( max("to_date")))
    .show()



    /*.join(employeesDF, "emp_no")
    .join(titlesDF, "emp_no")
    .orderBy("emp_no", "to_date")
    .show(10)*/

}