package wikipedia

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.apache.spark.rdd.RDD

case class WikipediaArticle(title: String, text: String)

object WikipediaRanking {

  val langs = List(
    "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
    "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

  val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Wikipedia Spark App")
  val sc: SparkContext = new SparkContext(conf)

  // Hint: use a combination of `sc.textFile`, `WikipediaData.filePath` and `WikipediaData.parse`
  val wikiRdd: RDD[WikipediaArticle] = sc.textFile(WikipediaData.filePath).map(s => WikipediaData.parse(s))

  /**
   * Checks for the language in the article text.
   */
  def contains(lang: String, article: WikipediaArticle) = article.text.split(" ").contains(lang)

  /** Returns the number of articles on which the language `lang` occurs.
   *  Hint1: consider using method `aggregate` on RDD[T].
   *  Hint2: should you count the "Java" language when you see "JavaScript"?
   *  Hint3: the only whitespaces are blanks " "
   *  Hint4: no need to search in the title :)
   */
  def occurrencesOfLang(lang: String, rdd: RDD[WikipediaArticle]): Int = {
    rdd.aggregate(0)((count, article) => if (contains(lang, article)) count + 1 else count, _ + _)
  }

  /* (1) Use `occurrencesOfLang` to compute the ranking of the languages
   *     (`val langs`) by determining the number of Wikipedia articles that
   *     mention each language at least once. Don't forget to sort the
   *     languages by their occurence, in decreasing order!
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  def rankLangs(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = {
    langs.map(lang => (lang, occurrencesOfLang(lang, rdd))).sortWith(_._2 > _._2)
  }

  /* Compute an inverted index of the set of articles, mapping each language
   * to the Wikipedia pages in which it occurs.
   *
   * (on the collect function)
   * When combining a map and filter one can rely on Partial Functions and Pattern Matching:
   *
   * langs.map(_ match {
   *   case lang contains(lang, article) => Some((lang, article))
   *   case _ => None
   * })
   *
   * or just use the collect method:
   *
   * langs.collect({ case lang if contains(lang, article) => (lang, article) }))
   *
   * Given a partial function pf, the following code:
   * someCollection.collect(pf)
   *
   * Is roughly equivalent to :
   * someCollection.filter(pf.isDefinedAt _).map(pf)
   */
  def makeIndex(langs: List[String], rdd: RDD[WikipediaArticle]): RDD[(String, Iterable[WikipediaArticle])] = {
    rdd.flatMap(article => langs.collect({ case lang if contains(lang, article) => (lang, article) })).groupByKey()
  }

  /* (2) Compute the language ranking again, but now using the inverted index. Can you notice
   *     a performance improvement?
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   *
   *   (On the use of mapValues)
   *   mapValues is only applicable for PairRDDs, meaning RDDs of the form RDD[(A, B)]. In that case, mapValues operates on the value only (the second part of the tuple), while map operates
   *   on the entire record (tuple of key and value).
   *   In other words, given f: B => C and rdd: RDD[(A, B]], these two are (almost) identical:
   *
   *   val result: RDD[(A, C)] = rdd.map { case (k, v) => (k, f(v)) }
   *   val result: RDD[(A, C)] = rdd.mapValues(f)
   *
   *   The latter is shorter and clearer, so when you just want to transform the values and keep the keys as-is, use mapValues.
   */
  def rankLangsUsingIndex(index: RDD[(String, Iterable[WikipediaArticle])]): List[(String, Int)] = {
    index.mapValues(articles => articles.size).sortBy(_._2, ascending = false).collect().toList
  }

  /* (3) Use `reduceByKey` so that the computation of the index and the ranking is combined.
   *     Can you notice an improvement in performance compared to measuring *both* the computation of the index
   *     and the computation of the ranking? If so, can you think of a reason?
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  def rankLangsReduceByKey(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = {
    rdd.flatMap(article => langs.collect({ case lang if contains(lang, article) => (lang, 1) })).reduceByKey(_ + _).sortBy(_._2, ascending = false).collect().toList
  }

  def main(args: Array[String]) {

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
  }

  val timing = new StringBuffer
  def timed[T](label: String, code: => T): T = {
    val start = System.currentTimeMillis()
    val result = code
    val stop = System.currentTimeMillis()
    timing.append(s"Processing $label took ${stop - start} ms.\n")
    result
  }
}
