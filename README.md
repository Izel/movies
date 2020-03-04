# Movies Project
This aim of this project is to make a brief analysis of a movies ranking dataset. 

## Dataset
The dataset consists of 3 `.dat` files (movies, ratings and users) and 1 README file for instructions.  
The files can be downloaded from https://grouplens.org/datasets/movielens/1m/ The files are not included in this project due to license restrictions of the dataset (check the README file when download).

Sample of movies.dat
```
726::Last Dance (1996)::Drama
727::War Stories (1995)::Documentary
728::Cold Comfort Farm (1995)::Comedy
```
Sample of users.dat
```
10::F::35::1::95370
11::F::25::1::04093
12::M::25::12::32793
```
Sample of rating.dat
```
1::1097::4::978301953
631::2752::3::975779334
4224::986::4::965315782
```

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

However, the technical details were interesting and forced me to read some detailed documentation and make 
a bit more research. The final implemented solution was not far from the first general view and I detail it below:

1. **Download and install the tools** Updating Java, installing IntelliJ. Getting familiarized with the IDE.
2. **Organize the project dependencies** Although `sbt` was intuitive and useful, I had to check the repositories, library versions and library names. Additionaly, make a basic tests to tune the configuration.
3. **Previous documentation** Read documentation about Scala and specially Spark (See references). The videos where useful.
4. **Setting up the repository** With some basic files. Initially private and released last day previous to the delivery.
5. **Create the `SparkSession`**, which is an object that abstracts all the previous objects to configure an Spark application. I had to read about this object because I didn't know how to use it.
6. **Load the data files into `DataFrame`.** Some data preprocessing was necessary to extract the Movies year from the tittle, loading just the users with `F` and `M`(at the beginning I thought it was a bit discriminatory and I suggest a friend who is expert in that topic for an advice, she told me for this case is Ok.). Finally, loading just the valid rankings [0,5].  This restrictions drove me to read about regular expressions, columns operations, filters and other functions.
7. **Tests** Create some unitary test and a Main.
8. **Create and apply filters** for users (Age) and Movies (Year)
9. **Make the joins** from all the `Dataframe`.
10.**Documentation** Was built through the project development.
11.**Refactor** Code style, refactor, improving code documentation.

### Problems
1. The files separator is a double colon `::`. Multiple characters in the files separators are not allowed and an exception was always thrown. 

```
Exception in thread "main" java.lang.IllegalArgumentException: Delimiter cannot be more than one character: ::
	at org.apache.spark.sql.execution.datasources.csv.CSVUtils$.toChar(CSVUtils.scala:118)
	at org.apache.spark.sql.execution.datasources.csv.CSVOptions.<init>(CSVOptions.scala:86)
	at org.apache.spark.sql.execution.datasources.csv.CSVOptions.<init>(CSVOptions.scala:41)
	at org.apache.spark.sql.execution.datasources.csv.CSVFileFormat.inferSchema(CSVFileFormat.scala:58)
	at org.apache.spark.sql.execution.datasources.DataSource$$anonfun$6.apply(DataSource.scala:180)
	at org.apache.spark.sql.execution.datasources.DataSource$$anonfun$6.apply(DataSource.scala:180)
	at scala.Option.orElse(Option.scala:289)
 ```

Despite of the time I spent looking for a solution to this problem, I didnt found a good way to deal with it, Due to the time limit,  decided to use the colon (`:`) as file separator. This solutionintroduces an extra `NULL` column for each `::` separator found in the file.  The `null` columns were removed once the data was loaded.


UserID|null1|MovieID|null2|Rating|null3|Timestamp
--|---|---|---|---|---|---|
|     1| null|   1193| null|     5| null|978300760
|     1| null|    661| null|     3| null|978302109
|     1| null|    914| null|     3| null|978301968


2. Data without header. It was not a big issue but a bit weird. The column names were added manually. 

3. The Movies year release was included in the title. It will be a problem for future manipulation of the data. One of the conditions was  _consider only movies, which were released after 1989._ Regular expressions and other DataFrame column functions were necessary.
 
4. Spark-core dependencies didn't work with `Scala 2.13.1`. The _Maven_ repository was very useful to check the library dependencies and names  


### To improve
1. The files loading. Avoid create the null columns to remove them later
2. More unit test.
3. Graph the result.

## The final output
Solution is a DataFrame ready to operate with.

MovieID|               Title|Year|Gender|        Avg_rating
---|---|---|---|---|
|      1|    Toy Story (1995)|1995|     F| 4.203155818540434
|      1|    Toy Story (1995)|1995|     M| 4.159599074787972
|      2|      Jumanji (1995)|1995|     F| 3.228187919463087
|      2|      Jumanji (1995)|1995|     M| 3.156182212581345
|      3|Grumpier Old Men ...|1995|     M|3.0207612456747404
|      3|Grumpier Old Men ...|1995|     F| 3.009259259259259
|      4|Waiting to Exhale...|1995|     F|2.9615384615384617
|      4|Waiting to Exhale...|1995|     M|2.4358974358974357
|      5|Father of the Bri...|1995|     M|2.9171597633136095
|      5|Father of the Bri...|1995|     F|3.0786516853932584
|      6|         Heat (1995)|1995|     F|3.6578947368421053
|      6|         Heat (1995)|1995|     M| 3.940771349862259
|      7|      Sabrina (1995)|1995|     F|3.5819209039548023
|      7|      Sabrina (1995)|1995|     M|3.2570093457943927
|      8| Tom and Huck (1995)|1995|     F|3.2777777777777777
|      8| Tom and Huck (1995)|1995|     M|2.8484848484848486
|      9| Sudden Death (1995)|1995|     F|               2.0
|      9| Sudden Death (1995)|1995|     M|2.6744186046511627
|     10|    GoldenEye (1995)|1995|     M| 3.525037936267071
|     10|    GoldenEye (1995)|1995|     F|3.3983050847457625


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

https://intellipaat.com/blog/what-is-apache-spark/

### IntelliJ Configuration
https://www.youtube.com/watch?v=ymLiCXs4dVg
