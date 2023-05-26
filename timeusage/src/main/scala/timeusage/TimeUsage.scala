package timeusage

import org.apache.spark.sql.*
import org.apache.spark.sql.types.*
import org.apache.log4j.{Logger, Level}
import scala.util.Properties.isWin

/** Main class */
object TimeUsage extends TimeUsageInterface:
  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.functions.*



  // Reduce Spark logging verbosity
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  if isWin then System.setProperty("hadoop.home.dir", System.getProperty("user.dir") + "\\winutils\\hadoop-3.3.1")

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Time Usage")
      .master("local")
      .getOrCreate()

  // For implicit conversions like converting RDDs to DataFrames
  import spark.implicits._
  import scala3encoders.given

  case class MovieRatings(movieName: String, rating: Double)
  case class MovieCritics(name: String, movieRatings: Seq[MovieRatings])
  val df =
    Seq(
      MovieCritics("Manuel", Seq(MovieRatings("Logan", 1.5), MovieRatings("Zoolander", 3), MovieRatings("John Wick", 2.5))),
      MovieCritics("John", Seq(MovieRatings("Logan", 2), MovieRatings("Zoolander", 3.5), MovieRatings("John Wick", 3))))
      .toDF()
  df.map(r => r)

//case class MovieRatings(movieName: String, rating: Double)
//
//case class MovieCritics(name: String, movieRatings: Seq[MovieRatings])

  /** Main function */
  def main(args: Array[String]): Unit =

    df.show()




    timeUsageByLifePeriod()
    spark.close()

  def timeUsageByLifePeriod(): Unit =
    val (columns, initDf) = read("src/main/resources/timeusage/atussum.csv")
    val (primaryNeedsColumns, workColumns, otherColumns) = classifiedColumns(columns)
//    primaryNeedsColumns.foreach(println)
    val summaryDf = timeUsageSummary(primaryNeedsColumns, workColumns, otherColumns, initDf)
    val finalDf = timeUsageGrouped(summaryDf)
    finalDf.show()

  /** @return The read DataFrame along with its column names. */
  def read(path: String): (List[String], DataFrame) =
    val df = spark.read.options(Map("header" -> "true", "inferSchema" -> "true")).csv(path)
    (df.schema.fields.map(_.name).toList, df)

  /** @return An RDD Row compatible with the schema produced by `dfSchema`
    * @param line Raw fields
    */
  def row(line: List[String]): Row =
    Row.fromSeq(line.head::line.tail.map(_.toDouble))

  /** @return The initial data frame columns partitioned in three groups: primary needs (sleeping, eating, etc.),
    *         work and other (leisure activities)
    *
    * @see https://www.kaggle.com/bls/american-time-use-survey
    *
    * The dataset contains the daily time (in minutes) people spent in various activities. For instance, the column
    * “t010101” contains the time spent sleeping, the column “t110101” contains the time spent eating and drinking, etc.
    *
    * This method groups related columns together:
    * 1. “primary needs” activities (sleeping, eating, etc.). These are the columns starting with “t01”, “t03”, “t11”,
    *    “t1801” and “t1803”.
    * 2. working activities. These are the columns starting with “t05” and “t1805”.
    * 3. other activities (leisure). These are the columns starting with “t02”, “t04”, “t06”, “t07”, “t08”, “t09”,
    *    “t10”, “t12”, “t13”, “t14”, “t15”, “t16” and “t18” (those which are not part of the previous groups only).
    */
  def classifiedColumns(columnNames: List[String]): (List[Column], List[Column], List[Column]) =
    var primay = List[Column]()
    var work = List[Column]()
    var other = List[Column]()

    val test = columnNames.map(c => {
      if c.startsWith("t01") || c.startsWith("t03") || c.startsWith("t11") || c.startsWith("t1801") || c.startsWith("t1803") then (1, c)
      else if c.startsWith("t05") || c.startsWith("t1805") then (2, c)
      else if c.startsWith("t02") || c.startsWith("t04") || c.startsWith("t06") || c.startsWith("t07") || c.startsWith("t08")
        || c.startsWith("t09") || c.startsWith("t10") || c.startsWith("t12") || c.startsWith("t13")
        || c.startsWith("t14") || c.startsWith("t15") || c.startsWith("t16") || c.startsWith("t18") then (3, c)
      else (-1, c)
    }).filter(_._1 > 0)
      .groupMap(_._1)(_._2)
//      .flatMap(r => (r._1, r._2.map(c => col(c))))

    val m = test.map(r => (r._1, r._2.map(col)))

    for (c <- columnNames) {
      if c.startsWith("t01") || c.startsWith("t03") || c.startsWith("t11") || c.startsWith("t1801") || c.startsWith("t1803") then {
        primay = primay :+ col(c)
//        println("add " + c)
      }
      else if c.startsWith("t05") || c.startsWith("t1805") then work = work :+ col(c)
      else if c.startsWith("t02") || c.startsWith("t04") || c.startsWith("t06") || c.startsWith("t07") || c.startsWith("t08")
        || c.startsWith("t09") || c.startsWith("t10") || c.startsWith("t12") || c.startsWith("t13")
        || c.startsWith("t14") || c.startsWith("t15") || c.startsWith("t16") || c.startsWith("t18") then other = other :+ col(c)
    }

//    println(primay.size)
    (m.getOrElse(1,List.empty), m.getOrElse(2,List.empty), m.getOrElse(3, List.empty))


  /** @return a projection of the initial DataFrame such that all columns containing hours spent on primary needs
    *         are summed together in a single column (and same for work and leisure). The “teage” column is also
    *         projected to three values: "young", "active", "elder".
    *
    * @param primaryNeedsColumns List of columns containing time spent on “primary needs”
    * @param workColumns List of columns containing time spent working
    * @param otherColumns List of columns containing time spent doing other activities
    * @param df DataFrame whose schema matches the given column lists
    *
    * This methods builds an intermediate DataFrame that sums up all the columns of each group of activity into
    * a single column.
    *
    * The resulting DataFrame should have the following columns:
    * - working: value computed from the “telfs” column of the given DataFrame:
    *   - "working" if 1 <= telfs < 3
    *   - "not working" otherwise
    * - sex: value computed from the “tesex” column of the given DataFrame:
    *   - "male" if tesex = 1, "female" otherwise
    * - age: value computed from the “teage” column of the given DataFrame:
    *   - "young" if 15 <= teage <= 22,
    *   - "active" if 23 <= teage <= 55,
    *   - "elder" otherwise
    * - primaryNeeds: sum of all the `primaryNeedsColumns`, in hours
    * - work: sum of all the `workColumns`, in hours
    * - other: sum of all the `otherColumns`, in hours
    *
    * Finally, the resulting DataFrame should exclude people that are not employable (ie telfs = 5).
    *
    * Note that the initial DataFrame contains time in ''minutes''. You have to convert it into ''hours''.
    */
  def timeUsageSummary(
    primaryNeedsColumns: List[Column],
    workColumns: List[Column],
    otherColumns: List[Column],
    df: DataFrame
  ): DataFrame =
    // Transform the data from the initial dataset into data that make
    // more sense for our use case
    // Hint: you can use the `when` and `otherwise` Spark functions
    // Hint: don’t forget to give your columns the expected name with the `as` method
    val workingStatusProjection: Column = when(df("telfs") >= 1 && df("telfs") < 3, "working").otherwise("not working") as "working"
    val sexProjection: Column = when(df("tesex").equalTo(1), "male").otherwise("female").as("sex")
    val ageProjection: Column = when(df("teage").between(15, 20), "young")
      .otherwise(when(df("teage").between(23, 55), "active")
      .otherwise("elder")).as("age")

    // Create columns that sum columns of the initial dataset
    // Hint: you want to create a complex column expression that sums other columns
    //       by using the `+` operator between them
    // Hint: don’t forget to convert the value to hours
    val calHours = (l: List[Column]) => l.reduce(_+_).divide(60)

    val primaryNeedsProjection: Column = calHours(primaryNeedsColumns) as "primaryNeeds"
    val workProjection: Column = calHours(workColumns) as "work"
    val otherProjection: Column = calHours(otherColumns) as "other"
    df
      .select(workingStatusProjection, sexProjection, ageProjection, primaryNeedsProjection, workProjection, otherProjection)
      .where($"telfs" <= 4) // Discard people who are not in labor force

  /** @return the average daily time (in hours) spent in primary needs, working or leisure, grouped by the different
    *         ages of life (young, active or elder), sex and working status.
    * @param summed DataFrame returned by `timeUsageSumByClass`
    *
    * The resulting DataFrame should have the following columns:
    * - working: the “working” column of the `summed` DataFrame,
    * - sex: the “sex” column of the `summed` DataFrame,
    * - age: the “age” column of the `summed` DataFrame,
    * - primaryNeeds: the average value of the “primaryNeeds” columns of all the people that have the same working
    *   status, sex and age, rounded with a scale of 1 (using the `round` function),
    * - work: the average value of the “work” columns of all the people that have the same working status, sex
    *   and age, rounded with a scale of 1 (using the `round` function),
    * - other: the average value of the “other” columns all the people that have the same working status, sex and
    *   age, rounded with a scale of 1 (using the `round` function).
    *
    * Finally, the resulting DataFrame should be sorted by working status, sex and age.
    */
  def timeUsageGrouped(summed: DataFrame): DataFrame =
    summed.groupBy("working", "sex", "age")
      .agg(round(avg("primaryNeeds"), 1) as "primaryNeeds",
        round(avg("work"), 1) as "work",
        round(avg("other"), 1) as "other")
      .orderBy("working", "sex", "age")

  /**
    * @return Same as `timeUsageGrouped`, but using a plain SQL query instead
    * @param summed DataFrame returned by `timeUsageSumByClass`
    */
  def timeUsageGroupedSql(summed: DataFrame): DataFrame =
    val viewName = s"summed"
    summed.createOrReplaceTempView(viewName)
    spark.sql(timeUsageGroupedSqlQuery(viewName))

  /** @return SQL query equivalent to the transformation implemented in `timeUsageGrouped`
    * @param viewName Name of the SQL view to use
    */
  def timeUsageGroupedSqlQuery(viewName: String): String =
    s"SELECT working, sex, age, ROUND(AVG(primaryNeeds), 1) AS primaryNeeds, ROUND(AVG(work), 1) AS work, ROUND(AVG(other), 1) AS other FROM $viewName GROUP BY working, sex, age"

  /**
    * @return A `Dataset[TimeUsageRow]` from the “untyped” `DataFrame`
    * @param timeUsageSummaryDf `DataFrame` returned by the `timeUsageSummary` method
    *
    * Hint: you should use the `getAs` method of `Row` to look up columns and
    * cast them at the same time.
    */
  def timeUsageSummaryTyped(timeUsageSummaryDf: DataFrame): Dataset[TimeUsageRow] =
    timeUsageSummaryDf.map(r => TimeUsageRow(r.getAs("working"), r.getAs("sex"), r.getAs("age"), r.getAs("primaryNeeds"), r.getAs("work"), r.getAs("other")))

  /**
    * @return Same as `timeUsageGrouped`, but using the typed API when possible
    * @param summed Dataset returned by the `timeUsageSummaryTyped` method
    *
    * Note that, though they have the same type (`Dataset[TimeUsageRow]`), the input
    * dataset contains one element per respondent, whereas the resulting dataset
    * contains one element per group (whose time spent on each activity kind has
    * been aggregated).
    *
    * Hint: you should use the `groupByKey` and `avg` methods.
    */
  def timeUsageGroupedTyped(summed: Dataset[TimeUsageRow]): Dataset[TimeUsageRow] = {
    import org.apache.spark.sql.expressions.scalalang.typed

    summed.groupByKey(r => (r.working, r.sex, r.age))
      .agg(round(typed.avg[TimeUsageRow](_.primaryNeeds), 1).as(Encoders.DOUBLE),
        round(typed.avg[TimeUsageRow](_.work), 1).as(Encoders.DOUBLE),
        round(typed.avg[TimeUsageRow](_.other), 1).as(Encoders.DOUBLE))
      .map(k => TimeUsageRow(k._1._1, k._1._2, k._1._3, k._2, k._3, k._4))
      .sort($"working", $"sex", $"age")
  }





/**
  * Models a row of the summarized data set
  * @param working Working status (either "working" or "not working")
  * @param sex Sex (either "male" or "female")
  * @param age Age (either "young", "active" or "elder")
  * @param primaryNeeds Number of daily hours spent on primary needs
  * @param work Number of daily hours spent on work
  * @param other Number of daily hours spent on other activities
  */
case class TimeUsageRow(
  working: String,
  sex: String,
  age: String,
  primaryNeeds: Double,
  work: Double,
  other: Double
)