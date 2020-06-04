package com.movies.spark.rdd

/**
  * Representation of a Movie.
  * @see [[/MoviesRDD.scala]]
  * @constructor Create a new movie.
  * @param id movie ID
  * @param name Movie Name
  * @param genres Genres
  */
case class Movie(val id: Int, val name: String, val genres: Seq[String])

/**
  * Representation of an User.
  * @see [[/MoviesRDD.scala]]
  * @consrcutor Create a user.
  * @param id user id
  * @param gender user gender
  * @param age user age
  * @param ocuppation user occupation
  * @param zipCode user zipcode
  */
case class User(val id: Int,
                val gender: String,
                val age: Int,
                val ocuppation: String,
                val zipCode: String)

/**
  * Class Rating. Represents an instance of a rating provided by an user to a movie.
  * @see [[/MoviesRDD.scala]]
  * @constructor Create a rating
  * @param userId ID of the user who provide the rating
  * @param movieId ID of the movie rated by the user
  * @param rating rate provided by the user to the movie
  * @param time timestamp of the rate.
  */
case class Rating(val userId: Int,
                  val movieId: Int,
                  val rating: Int,
                  val time: Int)
