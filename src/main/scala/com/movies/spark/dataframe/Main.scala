package com.movies.spark.dataframe

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}

object Main extends App {
  val spkSession = SparkSession
    .builder()
    .config("spark.master", "local")
    .appName("MoviesRanking")
    .getOrCreate()

  val data = MoviesDataFrame.filterMoviesByYear(1989, MoviesDataFrame.loadMovies(spkSession,"data/movies.dat"))
  //data.show(10)

  val usersDf = MoviesDataFrame.loadUsers(spkSession,"data/users.dat")
  //usersDf.show(10)

  val ratingsDf = MoviesDataFrame.loadRatings(spkSession,"data/ratings.dat")
  //ratingsDf.show(10)

  val rankings = MoviesDataFrame.moviesRankingByGender(data,usersDf, ratingsDf)
  rankings.show(10)
}
