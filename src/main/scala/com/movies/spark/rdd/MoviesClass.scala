package com.movies.spark.rdd

case class Movie(val id: Int, val name: String, val genres: Seq[String])

case class User(val id: Int,
                val gender: String,
                val age: Int,
                val ocuppation: String,
                val zipCode: String)

case class Rating(val userId: Int,
                  val movieId: Int,
                  val rating: Int,
                  val time: Int)
