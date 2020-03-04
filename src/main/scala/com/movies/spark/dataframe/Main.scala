package com.movies.spark.dataframe

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}

object Main extends App {
  val spkSession = SparkSession
    .builder()
    .config("spark.master", "local")
    .appName("MoviesRanking")
    .getOrCreate()

  val data = MoviesDataFrame.filterMoviesByYear(1985, MoviesDataFrame.loadMovies(spkSession,"data/movies.dat"))

  data.show(10)

  val usersDf = MoviesDataFrame.loadUsers(spkSession,"data/users.dat")
  usersDf.show(10)

}
