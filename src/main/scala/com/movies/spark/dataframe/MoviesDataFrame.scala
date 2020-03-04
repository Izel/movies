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
 *    MovieID::Title::Genres
 *    2261::One Crazy Summer (1986)::Comedy
 *    2262::About Last Night... (1986)::Comedy|Drama|Romance
 *    2263::Seventh Sign, The (1988)::Thriller
 *
 *  Users Dataset sample:*
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
   * Loads the .dat files into a DataFrame without any preprocessing
   * @param spkSession Spark session to be used for this App
   * @param pathToFile Path to .dat file
   * @return DataFrame with the .dat file content.
   */
  def loadDatFile(spkSession:SparkSession, pathToFile: String): DataFrame = {
    spkSession.read.option("sep", ":")
      .option("inferSchema", "true")
      .option("header", "false")
      .csv(pathToFile)
  }


  /**
   * Loads and prepares the movies data into a DataFrame. Preprocess the movie
   * release year in a new column.
   * @param spkSession Spark session to be used for this App
   * @param pathToFile path to movies.dat
   * @return Dataframe [id, name, categories, year]
   */
  def loadMovies(spkSession:SparkSession, pathToFile: String): DataFrame = {
    val colNames = Seq("MovieID", "null1", "Title", "null2", "Genres")
    val df = loadDatFile(spkSession,pathToFile)
      .toDF(colNames: _*)
      .drop("null1", "null2")

    //Extracts the year from the movie name and set it in a new column for future use
    df.withColumn("Year", regexp_extract(col("Title"),"(\\d{4})"
      ,1))
  }


  /**
   * Process and loads the users data in a DataFrame. Just keep the users with
   * genders: Female, Male
   * @param spkSession Spark session to be used for this App
   * @param pathToFile Path to .dat file
   * @return DataFrame [UserID, Gender, Age, Occupation, Zip-code]
   */
  def loadUsers(spkSession:SparkSession, pathToFile: String): DataFrame = {
    val colNames = Seq("UserID", "null1", "Gender", "null2",  "Age", "null3",
        "Occupation", "null4", "Zip-code")
    val genders= Seq("M", "F")

    loadDatFile(spkSession,pathToFile)
      .toDF(colNames:_*)
      .drop("null1","null2", "null3", "null4")
      // Keep records with genders in the genders Sequence
      .where(col("Gender").isin(genders:_*))
  }


  /**
   * Process and loads the ratings data in a DataFrame. Just keeps the ratings
   * between 0 and 5 [0,5]
   * @param spkSession Spark session to be used for this App
   * @param pathToFile Path to .dat file
   * @return DataFrame [UserID, MovieID, Rating, Timestamp]
   */
  def loadRatings(spkSession:SparkSession, pathToFile: String): DataFrame = {
    val colNames = Seq("UserID", "null1", "MovieID", "null2", "Rating",
      "null3", "Timestamp")

    loadDatFile(spkSession,pathToFile)
      .toDF(colNames:_*)
      .drop("null1","null2", "null3")
      .filter("rating <= 5")
  }


  /**
   * Filter movies released before an specific year
   * @param year The year to filter
   * @param df The DataFrame to filter
   * @return DataFrame that contains movies released after the specified year.
   */
  def filterMoviesByYear(year:Int, df: DataFrame): DataFrame = {
    df.filter(df("year") > year).toDF()
  }


  /**
   * Filter the users
   * @param df The DataFrame to filter
   * @return DataFrame with users ages between 18 and 49 (inclusive)
   */
  def filterUsersByAge(df: DataFrame ): DataFrame = {
   df.filter(df("age") > 17 and df("age") < 50).toDF()
  }


  /**
   * Obtain the average range per gender and year.
   * @param moviesDf The movies DataFrame.
   * @param usersDf  The users DataFrame.
   * @param ratingsDf The ratings DataFrame.
   * @return DataFrame with movies and average rates by gender.
   */
  def moviesRankingByGender(moviesDf:DataFrame, usersDf:DataFrame, ratingsDf:DataFrame): DataFrame = {
    ratingsDf
      .join(usersDf, "UserID")
      .join(moviesDf, "MovieID")
      .groupBy("MovieID", "Title", "Year", "Gender")
      .agg(avg(ratingsDf.col("rating")).alias("Avg_rating"))//, usersDf.col("Gender"))
      .orderBy("MovieID")
  }

}
