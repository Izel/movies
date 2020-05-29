package com.movies.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

object MoviesRDD {
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

  def loadUsersFile(spkCtx: SparkContext, pathToFile: String): Unit = {
    val lines = spkCtx.textFile(pathToFile)
    val data = lines.flatMap { line =>
      val p = line.split("::")
      Seq(User(p(0).toInt, p(1), p(2).toInt, p(3), p(4)))
    }
    data.take(5) foreach println
    println(data.count())
  }

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
