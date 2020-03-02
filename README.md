#Movies
This project aim is analyse briefly a movies ranking dataset. 

## Dataset
The dataset consist in 3 `.dat` files (movies, ratings and users) and 1 README for instructions.  
The files can be downloaded from https://grouplens.org/datasets/movielens/1m/ The files are not included in this project due to license restrictions of the dataset (check the README file when download).

## Environment
This was made using `Scala` language, `Spark` data framework and IntelliJ IDE. For dependencies control `sbt` was used. 
For more details, check the `build.sbt` file 

## Task
Obtain the average rating per gender and movie release year.

## Conditions
1. Include movies released after 1989  (movie year > 1989)
2. Include only ratings of people age between 18-49 (18 >= age <= 49)

## Solution


## References
The following documentation was used to understand Scala, Spark, sbt and IntelliJ.

### Spark
https://spark.apache.org/docs/latest/sql-getting-started.html

https://spark.apache.org/docs/2.2.1/sql-programming-guide.html

https://spark.apache.org/docs/latest/

https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.package

### IntelliJ Configuration
https://www.youtube.com/watch?v=ymLiCXs4dVg
