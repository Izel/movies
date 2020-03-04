package com.movies.spark.test.dataframe

import com.movies.spark.dataframe.MoviesDataFrame
import org.apache.spark.sql.SparkSession
import org.scalatest._

class MoviesDataFrameTest extends FlatSpec {
  val spkSession = SparkSession
    .builder()
    .appName("MoviesRanking")
    .config("spark.master", "local")
    .getOrCreate()

  "Movies dataset" should "have size 3883" in {
    val data = MoviesDataFrame.loadMovies(spkSession, "data/movies.dat")
    assert(data.count() == 3883)
  }

  it should "produce NoSuchElementException when head is invoked" in {
    assertThrows[NoSuchElementException] {
      Set.empty.head
    }
  }

}
