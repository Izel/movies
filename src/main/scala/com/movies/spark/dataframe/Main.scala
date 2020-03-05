package com.movies.spark.dataframe

import org.apache.spark.sql.{SQLContext, SparkSession}

object Main extends App {

  //Spark session for the app
  val spkSession = SparkSession
    .builder()
    .config("spark.master", "local")
    .appName("MoviesRanking")
    .getOrCreate()

  // Loading movies and Obtaining the released ones after 1989
  val data = MoviesDataFrame
    .filterMoviesByYear(
      1989,
      MoviesDataFrame.loadMovies(spkSession, "data/movies.dat")
    )
  // Loading the users but just ones with ages [18,49]
  val usersDf = MoviesDataFrame.filterUsersByAge(
    MoviesDataFrame.loadUsers(spkSession, "data/users.dat")
  )

  // Loading the movies rantings
  val ratingsDf = MoviesDataFrame.loadRatings(spkSession, "data/ratings.dat")
  //ratingsDf.show(5)

  val mDf = MoviesDataFrame.loadMoviesByGenres(spkSession, data)
  mDf.show(20)

  // Obtaining the rankings
  //val rankings = MoviesDataFrame
  //  .moviesRankingByGender(data, usersDf, ratingsDf)

  //rankings.show()

}
