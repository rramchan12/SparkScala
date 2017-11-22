package com.ravi.spark.sql

import scala.io.Source
import scala.io.Codec
import java.nio.charset.CodingErrorAction
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import java.sql.Timestamp
import org.joda.time.format.DateTimeFormat
import org.apache.spark.sql.functions._

object OldestFiveStarMovie {

  final case class Movie(userId: Int, movieId: Int, rating: Int, data: Timestamp)

  def mapper(line: String): Movie = {
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
    val fields = line.split("\t")

    val movie: Movie = Movie(fields(0).toInt, fields(1).toInt, fields(2).toInt, new Timestamp(fields(3).toInt))
    return movie
  }

  /** Load up a Map of movie IDs to movie names. */
  def loadMovieNames(): Map[Int, String] = {
    // Handle character encoding issues:
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    // Create a Map of Ints to Strings, and populate it from u.item.
    var movieNames: Map[Int, String] = Map()

    val lines = Source.fromFile("ml-100k/u.item").getLines()
    for (line <- lines) {
      var fields = line.split('|')
      if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
      }
    }

    return movieNames
  }

  def main(args: Array[String]) {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    //Set up context 
    //Use the new Spark Session Interface from Spark 2.0
    val spark = SparkSession
      .builder
      .appName("OldestMoviesDataSets")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") //To work around Spark 2.0 Bug in windows
      .getOrCreate()

    //Read each line construct, extract movieID and construct an RDD
    val lines = spark.sparkContext.textFile("ml-100k/u.data").map(mapper)

    //Convert to DataStream
    //Convert to Data Set
    import spark.implicits._ //Voodoo else it wont work
    val movieDS = lines.toDS

    val topMovieIds = movieDS.groupBy("movieID").agg(avg("rating"),max("data")).sort(desc("avg(rating)"),asc("max(data)"))
    topMovieIds.show();
    val top10 = topMovieIds.take(10)
    // Load Movie Name 
    val movieNames = loadMovieNames()

    //Print results
    for (result <- top10) {
      println(movieNames(result(0).asInstanceOf[Int]) + ":" + result(1))

    }
    spark.stop()
  }

}