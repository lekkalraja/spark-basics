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

  def transferTable(tables: List[String]) = tables foreach { tableName =>
    val tableDF = spark.read.format("jdbc").options(postgres + ("dbtable" -> s"public.$tableName")).load()
    tableDF.createOrReplaceTempView(tableName)
    tableDF.write
      .mode(SaveMode.Overwrite)
      .saveAsTable(tableName)
  }

  transferTable(List("employees", "salaries", "dept_manager", "titles"))

}
