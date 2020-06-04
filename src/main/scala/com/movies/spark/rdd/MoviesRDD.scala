package com.movies.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
  *  The Spark RDD documentation is available on:
  *  https://spark.apache.org/docs/latest/rdd-programming-guide.html
  *
  *  In this exercise, we will load three files in spark RDD. The with the content of three
  *  .dat files are available in https://grouplens.org/datasets/movielens/1m/
  *
  *  We want to:
  *  - Obtain the average range per gender Vs year and genre Vs year.
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
  *
  *  Users Dataset sample:*
  *    UserID::Gender::Age::Occupation::Zip-code
  *    25::M::18::4::01609
  *    26::M::25::7::23112
  *
  *  Rankings dataset sample:
  *    UserID::MovieID::Rating::Timestamp
  *    1::48::5::978824351
  *    1::1097::4::978301953
  *
  *  For more details about the dataset, check the README file available in the
  *  dataset location
  */
object MoviesRDD {

  /**
    * Load the movies file in a spark RDD
    * @param spkCtx spark context
    * @param pathToFile path to the movies.dat file
    */
  def loadMoviesFile(spkCtx: SparkContext, pathToFile: String): Unit = {
    val lines = spkCtx.textFile(pathToFile)
    val data = lines.flatMap { line =>
      val p = line.split("::")
      if (p(2).split('|').length == 0)
        Seq(Movie(p(0).toInt, p(1), Seq(p(2))))
      else
        Seq(Movie(p(0).toInt, p(1), p(2).split('|').toSeq))
    }
    data.take(5) foreach println
    println(data.count())
  }

  /**
    * Load the user file in a spark RDD
    * @param spkCtx Spark context
    * @param pathToFile Path tho users.dat file
    */
  def loadUsersFile(spkCtx: SparkContext, pathToFile: String): Unit = {
    val lines = spkCtx.textFile(pathToFile)
    val data = lines.flatMap { line =>
      val p = line.split("::")
      Seq(User(p(0).toInt, p(1), p(2).toInt, p(3), p(4)))
    }
    data.take(5) foreach println
    println(data.count())
  }

  /**
    * Load the ratings file in a spark RDD
    * @param spkCtx spark context
    * @param pathToFile path to ratings.dat file
    */
  def loadRatingsFile(spkCtx: SparkContext, pathToFile: String): Unit = {
    val lines = spkCtx.textFile(pathToFile)
    val data = lines.flatMap { line =>
      val p = line.split("::")
      Seq(Rating(p(0).toInt, p(1).toInt, p(2).toInt, p(3).toInt))
    }
    data.take(5) foreach println
    println(data.count())
  }

}
