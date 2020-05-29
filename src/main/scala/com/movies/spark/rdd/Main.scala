package com.movies.spark.rdd

import com.movies.spark.rdd.MoviesRDD.{
  loadMoviesFile,
  loadRatingsFile,
  loadUsersFile
}
import org.apache.spark.{SparkConf, SparkContext}

object Main extends App {
  val conf =
    new SparkConf().setAppName("MoviesRankingRDD").setMaster("local[*]")
  val sc = new SparkContext(conf)
  loadMoviesFile(sc, "data/movies.dat")
  loadUsersFile(sc, "data/users.dat")
  loadRatingsFile(sc, "data/ratings.dat")
}
