package com.spark.basics.lowlevel

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

import scala.io.Source

/**
 * RDD -> Resilient Distributed Datasets
 *   - Distributed typed collections of JVM Objects
 *   - The 'first citizens' of Spark: all higher level APIS reduce to RDDS
 *
 * Pros :
 *    - Partitioning can be controlled
 *    - Order of elements can be controlled
 *    - Order of operations matters for performance
 *
 * Cons : hard to work with
 *    - for complex operations, need to know the internals of spark
 *    - poor APIs for quick data processing
 *
 * For 99% of operations, use the DataFrame/Dataset APIs
 */
object RDDs extends App {

  private val spark: SparkSession = SparkSession.builder()
    .appName("RDDS")
    .config("spark.master", "local")
    .getOrCreate()

  private val sc: SparkContext = spark.sparkContext

  /**
   * Creating RDD's
   */
  //1. Parallelize an existing collection
  private val numbers: Range.Inclusive = 1 to 100000
  private val numbersRDD: RDD[Int] = sc.parallelize(numbers)
  //numbersRDD.foreach(println)

  //2. Reading from files
  case class Stock(symbol: String, date: String, price: Double)
  private val stocks: List[Stock] = Source.fromFile("src/main/resources/data/stocks.csv")
    .getLines()
    .drop(1)
    .map(line => line.split(","))
    .map(words => Stock(words(0), words(1), words(2).toDouble))
    .toList
  private val stocksRDD: RDD[Stock] = sc.parallelize(stocks)
  //stocksRDD.foreach(println)

  //2b - reading from files
  private val unTypedStocksRDD: RDD[String] = sc.textFile("src/main/resources/data/stocks.csv")

  private val typedStockRDD: RDD[Stock] = unTypedStocksRDD
    .filter(line => !line.contains("symbol")) // To Skip the Header
    .map(_.split(","))
    .map(words => Stock(words(0), words(1), words(2).toDouble))
  //typedStockRDD.foreach(println)

  // 3 - read from a DF

  private val stockDF: DataFrame = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/main/resources/data/stocks.csv")

  import spark.implicits._

  private val stockDS: Dataset[Stock] = stockDF.as[Stock]
  private val rdd: RDD[Stock] = stockDS.rdd // Can Keep Type
  //rdd.foreach(println)
  private val rdd1: RDD[Row] = stockDF.rdd // will loose Type
  //rdd1.foreach(println)

  // RDD -> DS
  private val stockDS1: Dataset[Stock] = spark.createDataset(rdd) // can keep the type info

  // RDD -> DF
  private val stockDF1: DataFrame = rdd.toDF("symbol", "date", "price") // will loose type info

  /**
   * Transformations
   */
  // counting
  private val msftCount: Long = rdd.filter(_.symbol == "MSFT") // Transformation - Lazy Evaluation
    .count() // Action - Eager Evaluation
  //println(s"Count of Microsoft stocks $msftCount")

  //distinct
  rdd.map(_.symbol).distinct()
    //.foreach(println)

  //min and max
 // implicit private val ordering: Ordering[Stock] = Ordering.fromLessThan[Stock]((sa, sb) => sa.price < sb.price) // 1 - way
  //implicit private val ordering: Ordering[Stock] = Ordering.fromLessThan((sa, sb) => sa.price < sb.price) // 2 - way
  implicit private val ordering: Ordering[Stock] = Ordering.fromLessThan((sa : Stock, sb : Stock) => sa.price < sb.price) // 3 - way
  private val minStock: Stock = rdd.min()
  private val maxStock: Stock = rdd.max()
  //println(s"Min Stock $minStock")
  //println(s"Max Stock $maxStock")

  //reduce
  private val sumOfNumbers: Int = numbersRDD.reduce(_ + _)

  //grouping (********* very expensive ****************)
  private val groupByCompany: RDD[(String, Iterable[Stock])] = rdd.groupBy(_.symbol)
  //groupByCompany.foreach(println)

  // Partitioning

  rdd.toDF().write.mode(SaveMode.Overwrite)
    //.save("src/main/resources/data/stocks1p") // Will create one file because have only one Partition
    private val repartitionRDD: RDD[Stock] = rdd.repartition(30)
  repartitionRDD.toDF().write.mode(SaveMode.Overwrite)
    //.save("src/main/resources/data/stocks30p") // will create 30 files

  /**
   * Repartitioning is EXPENSIVE. Involves Shuffling
   * Best Practice : Partition Early, then process that
   * Size of a partition good to have between 10 to 100 MB
   */

  //coalesce
  private val coalescedRDD: RDD[Stock] = repartitionRDD.coalesce(15) // does NOT involve shuffling
  coalescedRDD.toDF.write.mode(SaveMode.Overwrite)
   // .save("src/main/resources/data/stocks15p") // will create 15 files

  /**
   * Exercises
   */

  /**
   * 1. Read the movies.json as an RDD.
   */
  case class Movie(Title: String, Major_Genre: String, IMDB_Rating: String)

  private val moviesRDD: RDD[Movie] = spark.read.option("inferSchema", "true")
    .json("src/main/resources/data/movies.json").as[Movie].rdd
  //moviesRDD.foreach(println)

  /**
   * 2. Show the distinct genres as an RDD
   */
  private val distinctGenres: RDD[String] = moviesRDD.map(_.Major_Genre).filter(_ != null).distinct()
  //distinctGenres.foreach(println)

  /**
   * 3. Select all the movies in the Drama Genre with IMDB rating > 6
   */
  moviesRDD.filter(_.Major_Genre == "Drama")
    .filter(movie => movie.IMDB_Rating != null && movie.IMDB_Rating.toDouble > 6)
    //.foreach(println)

  /**
   * 4. Show the average rating of movies by genre
   */
    case class GenreAvgRating(genre: String, rating: Double)

  private val groupByGenre: RDD[(String, Iterable[Movie])] = moviesRDD.filter(_.Major_Genre != null).groupBy(_.Major_Genre)
  private val avgRating: RDD[GenreAvgRating] = groupByGenre.map {
    case (genre, movies) => GenreAvgRating(genre, (movies.filter(_.IMDB_Rating != null)
      .map(_.IMDB_Rating.toDouble).sum) / movies.size)
  }
  avgRating.foreach(println)
}