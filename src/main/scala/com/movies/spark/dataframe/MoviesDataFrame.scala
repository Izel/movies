package com.movies.spark.dataframe

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 *  The Spark DataFrame documentation is available on:
 *  https://spark.apache.org/docs/1.6.1/api/java/org/apache/spark/sql/DataFrame.html
 *
 *  In this exercise,three DataFrames will be created with the content of three
 *  .dat files
 *  available in https://grouplens.org/datasets/movielens/1m/
 *
 *  We want to:
 *  - Obtain the average range per gender and year.
 *
 *  Constraints:
 *  - Just keep in count users with age between 18 and 49 years old.
 *  - Just calculate the rankings for movies released after 1985.
 *
 *  The Dataset files don't have header and the fields separator is a double
 *  character "::"
 *
 *  Movies dataset sample:
 *
 *    2261::One Crazy Summer (1986)::Comedy
 *    2262::About Last Night... (1986)::Comedy|Drama|Romance
 *    2263::Seventh Sign, The (1988)::Thriller
 *
 *  Users Dataset sample:
 *    UserID::MovieID::Rating::Timestamp
 *    UserID::Gender::Age::Occupation::Zip-code
 *    25::M::18::4::01609
 *    26::M::25::7::23112
 *    27::M::25::11::19130
 *
 *  Rankings dataset sample:
 *    UserID::MovieID::Rating::Timestamp
 *    1::48::5::978824351
 *    1::1097::4::978301953
 *    1::1721::4::978300055
 *
 *  For more details about the detaset, check the README file available in the
 *  dataset location
 */
object MoviesDataFrame {

  /**
   * Loads and prepares the movies data into a dataframe. Preprocess the movie
   * release year in a new column.
   * @param spkSession
   * @param pathToFile path to movies.dat
   * @return Dataframe [id, name, categories, year]
   */
  def loadMovies(spkSession:SparkSession, pathToFile: String): DataFrame = {
    val colNames = Seq("id", "null1", "name", "null2", "categories")
    val df = spkSession.read.option("sep", ":")
      .option("inferSchema", "true")
      .option("header", "false")
      .csv(pathToFile)
      .toDF(colNames: _*)
      .drop("null1", "null2")

    val moviesDf = df.withColumn("year",  regexp_extract(col("name"),"(\\d{4})", 1))
    moviesDf
  }


  def filterMoviesByYear(year:Int, df: DataFrame): DataFrame = {
    val moviesDf = df.filter(df("year") > year).toDF()
    moviesDf
  }


  def filterUsersByAge(df: DataFrame) : DataFrame = {
    val usersDF = df.filter(df("age") > 18 and df("age") < 50).toDF()
    usersDF
  }


}
