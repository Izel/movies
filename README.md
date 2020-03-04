# Movies Project
This aim of this project is to make a brief analysis of a movies ranking dataset. 

## Dataset
The dataset consists of 3 `.dat` files (movies, ratings and users) and 1 README file for instructions.  
The files can be downloaded from https://grouplens.org/datasets/movielens/1m/ The files are not included in this project due to license restrictions of the dataset (check the README file when download).

## Environment
This project was made using `Scala` language, `Spark` data framework and IntelliJ IDE. For dependencies control `sbt` was used. For more details, check the `build.sbt` file.

## Task
Obtain the average rating per gender and movie release year.

## Conditions
1. Include movies released after 1989  (movie year > 1989)
2. Include only ratings of people age between 18-49 (18 >= age <= 49)

## Solution
Although the exercise looks very simple it has some technical challenges. A first general view of the solution was:

1. Create the `SparkContext`, `SparkConfig`
2. Load the data files into RDDs
3. Make the filters  for age (users) and year (Movies)
4. Make the necessary joins

However, the technical details were interesting and forced me  to read some detailed documentation and make 
a bit more research. The final implemented solution was not far from the first general view and I detail it below:

1. **Download and install the tools** Updating Java, installing IntelliJ. Getting familiarized with the IDE.
2. **Organize the project dependencies** Check the repositories, library versions and library names. Making basic tests.
3. **Previous documentation** Read documentation about Scala and specially Spark. The videos where useful.
4. **Setting up the repository** With some basic files. Initially private and released last day previous to the delivery.
5. **Create the `SparkSession`**, which is an object that abstracts all the previous objects to configure an Spark application.
6. **Tests** Create some unitary test and a Main.
7. **Load the data files into `DataFrame`.** Some data preprocessing was necessary to extract the Movies year from the tittle, loading just the users with `F` and `M`
(At the beginning I thought it was a bit discriminatory and I suggest a friend who is expert in that topic for an advice, she told me for this case is Ok.)
and finally loading just the valid rankings [0,5].  This restrictions drove me to read about regular expressions, columns operations, filters and other functions
8. **Create and apply filters** for users (Age) and Movies (Year)
9. **Make the joins** from all the `Dataframe`.
10. **Documentation** Was built through the project development.
11.**Refactor** Code style, refactor, improving code documentation.

### Problems
The files separator is a double colon `::`. Multiple characters in the files separators are not allowed and an exception was always thrown. 
Despite of the time I spent looking for a solution to this problem, I didnt found a good way to deal with it, Due to the time limit,  decided 
to use the `":"` as file separator, which includes an extra `NULL` column for each `"::"` found in the file.  The `null` columns were removed once
the data was loaded.

Data without header. It was not a big issue but a bit weird. The column names were added manually. 

The Movies year release was included in the title. It will be a problem for future manipulation of the data. One of the conditions was
 _consider only movies, which were released after 1989._ Regular expressions and other DataFrame column functions were necessary.
 
Spark-core dependencies didn't work with `Scala 2.13.1`. The _Maven_ repository was very useful to check the library dependencies and names  

### To improve
1. The files loading. Avoid create the null columns to remove them later
2. More data verification.
3. More unit test.

## References
The following documentation was used to understand Scala, Spark, sbt and IntelliJ.

### Scala
https://docs.scala-lang.org/getting-started/index.html

https://docs.scala-lang.org/tour/tour-of-scala.html

https://docs.scala-lang.org/style/index.html

### Spark
https://spark.apache.org/docs/latest/sql-getting-started.html

https://spark.apache.org/docs/2.2.1/sql-programming-guide.html

https://spark.apache.org/docs/latest/

https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.package

### IntelliJ Configuration
https://www.youtube.com/watch?v=ymLiCXs4dVg
