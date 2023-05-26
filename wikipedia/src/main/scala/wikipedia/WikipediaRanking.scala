package wikipedia

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.*
import org.apache.log4j.{Logger, Level}

import org.apache.spark.rdd.RDD
import scala.util.Properties.isWin

case class WikipediaArticle(title: String, text: String):
  /**
    * @return Whether the text of this article mentions `lang` or not
    * @param lang Language to look for (e.g. "Scala")
    */
  def mentionsLanguage(lang: String): Boolean = text.split(' ').contains(lang)

object WikipediaRanking extends WikipediaRankingInterface:
  // Reduce Spark logging verbosity
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  if isWin then System.setProperty("hadoop.home.dir", System.getProperty("user.dir") + "\\winutils\\hadoop-3.3.1")




  val langs = List(
    "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
    "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

  val conf: SparkConf = new SparkConf().setAppName("wiki").setMaster("local[2]")
  val sc: SparkContext = new SparkContext(conf)
  // Hint: use a combination of `sc.parallelize`, `WikipediaData.lines` and `WikipediaData.parse`
  val wikiRdd: RDD[WikipediaArticle] = sc.parallelize(WikipediaData.lines)
    .map(line => WikipediaData.parse(line))






  val data = Seq(("abc", 20, "NY"), ("qxt", 18, "LDN"))
  val rdd = sc.parallelize(data)
    rdd.map(r => {
      val f = if (r._2 > 18) "Y" else "N"
      (r._1, r._2, r._3, f)
    }).foreach(println)

  case class MovieRatings(movieName: String, rating: Double)

  case class MovieCritics(name: String, movieRatings: Seq[MovieRatings])

  val movies_critics = Seq(
    MovieCritics("Manuel", Seq(MovieRatings("Logan", 1.5), MovieRatings("Zoolander", 3), MovieRatings("John Wick", 2.5))),
    MovieCritics("John", Seq(MovieRatings("Logan", 2), MovieRatings("Zoolander", 3.5), MovieRatings("John Wick", 3))))
  val ratings = movies_critics.toDF


  /** Returns the number of articles on which the language `lang` occurs.
   *  Hint1: consider using method `aggregate` on RDD[T].
   *  Hint2: consider using method `mentionsLanguage` on `WikipediaArticle`
   */
  def occurrencesOfLang(lang: String, rdd: RDD[WikipediaArticle]): Int =
    rdd.filter(r => r.mentionsLanguage(lang)).count().toInt

  /* (1) Use `occurrencesOfLang` to compute the ranking of the languages
   *     (`val langs`) by determining the number of Wikipedia articles that
   *     mention each language at least once. Don't forget to sort the
   *     languages by their occurrence, in decreasing order!
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  def rankLangs(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] =
    langs.map(lang => (lang, occurrencesOfLang(lang, rdd)))
      .sortBy(_._2)
      .reverse

  /* Compute an inverted index of the set of articles, mapping each language
   * to the Wikipedia pages in which it occurs.
   */
  def makeIndex(langs: List[String], rdd: RDD[WikipediaArticle]): RDD[(String, Iterable[WikipediaArticle])] =
    rdd.flatMap(r =>
      langs.filter(lang => r.mentionsLanguage(lang))
      .map(lang => (lang, r)))
      .groupByKey()

  /* (2) Compute the language ranking again, but now using the inverted index. Can you notice
   *     a performance improvement?
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  def rankLangsUsingIndex(index: RDD[(String, Iterable[WikipediaArticle])]): List[(String, Int)] =
    index.mapValues(v => v.size)
      .sortBy(pair => pair._2, false)
      .collect()
      .toList

  /* (3) Use `reduceByKey` so that the computation of the index and the ranking are combined.
   *     Can you notice an improvement in performance compared to measuring *both* the computation of the index
   *     and the computation of the ranking? If so, can you think of a reason?
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  def rankLangsReduceByKey(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] =
//    rdd.flatMap(r =>
//      langs.filter(lang => r.mentionsLanguage(lang))
//      .map(lang => (lang, 1)))
//      .reduceByKey(_+_)
//      .sortBy(_._2, false)
//      .collect()
//      .toList
//      .sortBy(_._2)
    rdd.flatMap(r =>
      langs.filter(lang => r.mentionsLanguage(lang))
        .map(lang => (lang, 1)))
      .countByKey()
      .map((k, v) => (k, v.toInt))
      .toList
      .sortBy(_._2)
      .reverse

  def main(args: Array[String]): Unit =

    /* Languages ranked according to (1) */
    val langsRanked: List[(String, Int)] = timed("Part 1: naive ranking", rankLangs(langs, wikiRdd))

    /* An inverted index mapping languages to wikipedia pages on which they appear */
    def index: RDD[(String, Iterable[WikipediaArticle])] = makeIndex(langs, wikiRdd)

    /* Languages ranked according to (2), using the inverted index */
    val langsRanked2: List[(String, Int)] = timed("Part 2: ranking using inverted index", rankLangsUsingIndex(index))

    /* Languages ranked according to (3) */
    val langsRanked3: List[(String, Int)] = timed("Part 3: ranking using reduceByKey", rankLangsReduceByKey(langs, wikiRdd))

    /* Output the speed of each ranking */
    println(timing)
    sc.stop()

  val timing = new StringBuffer
  def timed[T](label: String, code: => T): T =
    val start = System.currentTimeMillis()
    val result = code
    val stop = System.currentTimeMillis()
    timing.append(s"Processing $label took ${stop - start} ms.\n")
    result
