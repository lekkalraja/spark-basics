package com.spark.basics.sql

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object SparkSQL extends App {

  private val spark: SparkSession = SparkSession.builder()
    .appName("Spark SQL")
    .config("spark.master", "local")
    .config("spark.sql.warehouse.dir", "src/main/resources/warehouse")
    .getOrCreate()

  private val carsDF: DataFrame = spark.read.option("inferSchema", "true").json("src/main/resources/data/cars.json")

  /**
   * Use Spark-SQL
   */
  carsDF.createOrReplaceTempView("cars")
  private val americanCars: DataFrame = spark.sql(
    """
      |select * from cars where origin = 'USA'
      |""".stripMargin)
  //americanCars.show

  /**
   * SQL's will sequentially as we specified
   */
  spark.sql("create database rtjvm")
  spark.sql("use rtjvm")
  private val databases: DataFrame = spark.sql("show databases")
  //databases.show

  /**
   * Transfer tables from a DB (Postgres) to spark tables
   */
  private val postgres = Map(
    "driver" -> "org.postgresql.Driver",
    "url" -> "jdbc:postgresql://localhost:5432/rtjvm",
    "user" -> "docker",
    "password" -> "docker",
  )

  def transferTable(tables: List[String]): Unit = tables foreach { tableName =>
    val tableDF = spark.read.format("jdbc").options(postgres + ("dbtable" -> s"public.$tableName")).load()
    tableDF.createOrReplaceTempView(tableName)
    //tableDF.write.mode(SaveMode.Ignore).saveAsTable(tableName)
  }

  transferTable(List("employees", "salaries", "dept_manager", "titles"))

  spark.read.table("employees")
    //.show

  /**
   * Exercises
   */

  /**
   * 1. Read the movies DF and store it as a Spark table in the rtjvm database
   */

  spark.read.option("inferSchema", "true").json("src/main/resources/data/movies.json")
    .write.mode(SaveMode.Overwrite)
   // .saveAsTable("movies")

  spark.read
    //.table("movies")
    //.show

  /**
   * 2. Count how many employees were hired in between Jan 1 2000 and Jan 1 2001 (output = 1)
   */
  spark.read.table("employees")
    .sqlContext
    .sql(
      """
        | select count(*) from employees
        | where hire_date >= CAST('2000-01-01' AS date) AND hire_date  <= CAST('2001-01-01' AS date)
        |""".stripMargin)
    // .show

  /**
   * 3. Show the average salaries for the employees hired in between those dates, grouped by department
   */
  spark.sql(
    """
      | select de.dept_no, avg(sal.salaries)
      | from employees e,  dept_emp de, salaries sal
      | where hire_date >= CAST('2000-01-01' AS date) AND hire_date  <= CAST('2001-01-01' AS date)
      | and e.emp_no = de.emp_no and e.emp_no = sal.emp_no
      | group by de.dept_no
      |""".stripMargin)
  //  .show
}
