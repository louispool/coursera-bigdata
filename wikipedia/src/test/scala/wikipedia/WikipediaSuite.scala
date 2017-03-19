package wikipedia

import org.scalatest.{FunSuite, BeforeAndAfterAll}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

@RunWith(classOf[JUnitRunner])
class WikipediaSuite extends FunSuite with BeforeAndAfterAll {

  def initializeWikipediaRanking(): Boolean =
    try {
      WikipediaRanking
      true
    } catch {
      case ex: Throwable =>
        println(ex.getMessage)
        ex.printStackTrace()
        false
    }

  override def afterAll(): Unit = {
    assert(initializeWikipediaRanking(), " -- did you fill in all the values in WikipediaRanking (conf, sc, wikiRdd)?")
    import WikipediaRanking._
    sc.stop()
  }

  // Conditions:
  // (1) the language stats contain the same elements
  // (2) they are ordered (and the order doesn't matter if there are several languages with the same count)
  def equivalentAndOrdered(given: List[(String, Int)], expected: List[(String, Int)]): Boolean = {
    /* (1) */ (given.toSet == expected.toSet) &&
    /* (2) */!(given zip given.tail).exists({ case ((_, occ1), (_, occ2)) => occ1 < occ2 })
  }

  test("'occurrencesOfLang' should work for (specific) RDD with one element") {
    assert(initializeWikipediaRanking(), " -- did you fill in all the values in WikipediaRanking (conf, sc, wikiRdd)?")
    import WikipediaRanking._
    val rdd = sc.parallelize(Seq(WikipediaArticle("title", "Java Jakarta")))
    val res = (occurrencesOfLang("Java", rdd) == 1)
    assert(res, "occurrencesOfLang given (specific) RDD with one element should equal to 1")
  }

  test("'occurrencesOfLang' should work for (specific) RDD with more than one element element") {
    assert(initializeWikipediaRanking(), " -- did you fill in all the values in WikipediaRanking (conf, sc, wikiRdd)?")
    import WikipediaRanking._
    val rdd = sc.parallelize(Seq(WikipediaArticle("1", "Java Jakarta is not actually a Programming Language"),
                                 WikipediaArticle("2", "Scala is a mixed approach to functional programming"),
                                 WikipediaArticle("3", "Spark is awesome, but Flink does Streaming a bit better"),
                                 WikipediaArticle("4", "JavaScript is an interpreted language, and one can argue the same for all JVM-based languages such as Scala"),
                                 WikipediaArticle("5", "Unfortunately, Scala is a bit slow if you always follow functional principles")))

    assert((occurrencesOfLang("Java", rdd) == 1))
    assert((occurrencesOfLang("Scala", rdd) == 3))
  }

  test("'rankLangs' should work for RDD with two elements") {
    assert(initializeWikipediaRanking(), " -- did you fill in all the values in WikipediaRanking (conf, sc, wikiRdd)?")
    import WikipediaRanking._
    val langs = List("Scala", "Java")
    val rdd = sc.parallelize(List(WikipediaArticle("1", "Scala is great, Python is pretty neat too, and more widely adopted"),
                                  WikipediaArticle("2", "Java is a brilliant language, Scala can be too")))
    val ranked = rankLangs(langs, rdd)
    val res = ranked.head._1 == "Scala"
    assert(res)
  }

  test("'rankLangs' should work for RDD with more than two elements") {
    assert(initializeWikipediaRanking(), " -- did you fill in all the values in WikipediaRanking (conf, sc, wikiRdd)?")
    import WikipediaRanking._
    val langs = List("Scala", "Java", "JavaScript", "Python", "Occam")
    val rdd = sc.parallelize(List(WikipediaArticle("1", "Scala is great, Python is pretty neat too, and more widely adopted"),
                                  WikipediaArticle("2", "Java is a brilliant language, Scala can be too"),
                                  WikipediaArticle("3", "Java Jakarta is not actually a Programming Language"),
                                  WikipediaArticle("4", "Scala is a mixed approach to functional programming"),
                                  WikipediaArticle("5", "Spark is awesome, but Flink does Streaming a bit better"),
                                  WikipediaArticle("6", "JavaScript is an interpreted language, and one can argue the same for all JVM-based languages such as Scala"),
                                  WikipediaArticle("7", "Unfortunately, Scala is a bit slow if you always follow functional principles")))
    val ranked = rankLangs(langs, rdd)

    assert(ranked.head._1 === "Scala")
    assert(ranked.head._2 === 5)
    assert(ranked.tail.head._1 === "Java")
    assert(ranked.tail.head._2 === 2)
    assert(ranked.tail.tail.head._1 === "JavaScript")
    assert(ranked.tail.tail.head._2 === 1)
    assert(ranked.tail.tail.tail.head._1 === "Python")
    assert(ranked.tail.tail.tail.head._2 === 1)
    assert(ranked.tail.tail.tail.tail.head._1 === "Occam")
    assert(ranked.tail.tail.tail.tail.head._2 === 0)
  }

  test("'makeIndex' creates a simple index with two entries") {
    assert(initializeWikipediaRanking(), " -- did you fill in all the values in WikipediaRanking (conf, sc, wikiRdd)?")
    import WikipediaRanking._
    val langs = List("Scala", "Java")
    val articles = List(
        WikipediaArticle("1","Groovy is pretty interesting, and so is Erlang"),
        WikipediaArticle("2","Scala and Java run on the JVM"),
        WikipediaArticle("3","Scala is not purely functional")
      )
    val rdd = sc.parallelize(articles)
    val index = makeIndex(langs, rdd)
    val res = index.count() == 2
    assert(res)
  }

  test("'makeIndex' creates a simple index with more than two entries") {
    assert(initializeWikipediaRanking(), " -- did you fill in all the values in WikipediaRanking (conf, sc, wikiRdd)?")
    import WikipediaRanking._
    val langs = List("Scala", "Java", "Groovy", "Erlang", "JavaScript")
    val articles = List(WikipediaArticle("1", "Groovy is pretty interesting, and so is Erlang"),
                        WikipediaArticle("2", "Scala and Java run on the JVM"),
                        WikipediaArticle("3", "Scala is not purely functional"),
                        WikipediaArticle("4", "Scala is great, Python is pretty neat too"),
                        WikipediaArticle("6", "Java is a brilliant language, Scala can be too"),
                        WikipediaArticle("7", "Java Jakarta is not actually a Programming Language"),
                        WikipediaArticle("8", "Scala is a practical approach to functional programming"),
                        WikipediaArticle("9", "Spark is awesome, but Flink does Streaming a bit better"),
                        WikipediaArticle("10","JavaScript is an interpreted language, and one can argue the same for all JVM-based languages such as Scala"),
                        WikipediaArticle("11","Unfortunately, Scala is a bit slow if you always follow functional principles"))
    val rdd = sc.parallelize(articles)
    val index = makeIndex(langs, rdd)
    val res = index.count() == 5
    assert(res)
  }

  test("'rankLangsUsingIndex' should work for a simple RDD with three elements") {
    assert(initializeWikipediaRanking(), " -- did you fill in all the values in WikipediaRanking (conf, sc, wikiRdd)?")
    import WikipediaRanking._
    val langs = List("Scala", "Java")
    val articles = List(
        WikipediaArticle("1","Groovy is pretty interesting, and so is Erlang"),
        WikipediaArticle("2","Scala and Java run on the JVM"),
        WikipediaArticle("3","Scala is not purely functional")
      )
    val rdd = sc.parallelize(articles)
    val index = makeIndex(langs, rdd)
    val ranked = rankLangsUsingIndex(index)
    val res = (ranked.head._1 == "Scala")
    assert(res)
  }

  test("'rankLangsUsingIndex' should work for a simple RDD with more than three elements") {
    assert(initializeWikipediaRanking(), " -- did you fill in all the values in WikipediaRanking (conf, sc, wikiRdd)?")
    import WikipediaRanking._
    val langs = List("Scala", "Java", "JavaScript", "Erlang", "Groovy", "Occam", "Python")
    val articles = List(WikipediaArticle("1", "Groovy is pretty interesting, and so is Erlang"),
                        WikipediaArticle("2", "Scala and Java run on the JVM"),
                        WikipediaArticle("3", "Scala is not purely functional"),
                        WikipediaArticle("4", "Scala is great, Python is pretty neat too"),
                        WikipediaArticle("6", "Java is a brilliant language, Scala can be too"),
                        WikipediaArticle("7", "Java Jakarta is not actually a Programming Language"),
                        WikipediaArticle("8", "Scala is a practical approach to functional programming"),
                        WikipediaArticle("9", "Spark is awesome, but Flink does Streaming a bit better"),
                        WikipediaArticle("10", "JavaScript is an interpreted language, and one can argue the same for all Java Virtual Machine based languages such as Scala"),
                        WikipediaArticle("11", "Unfortunately, Scala is a bit slow if you always follow functional principles"),
                        WikipediaArticle("12", "We don't talk about Python in this course"))
    val rdd = sc.parallelize(articles)
    val index = makeIndex(langs, rdd)
    val ranked = rankLangsUsingIndex(index)
    assert(ranked.head._1 === "Scala")
    assert(ranked.head._2 === 7)
    assert(ranked.tail.head._1 === "Java")
    assert(ranked.tail.head._2 === 4)
    assert(ranked.tail.tail.head._1 === "Python")
    assert(ranked.tail.tail.head._2 === 2)
    //assert(ranked.tail.tail.tail.head._1 === "JavaScript") //Sorting is not stable
    assert(ranked.tail.tail.tail.head._2 === 1)
    //assert(ranked.tail.tail.tail.tail.head._1 === "Erlang")
    assert(ranked.tail.tail.tail.tail.head._2 === 1)
    //assert(ranked.tail.tail.tail.tail.tail.head._1 === "Groovy")
    assert(ranked.tail.tail.tail.tail.tail.head._2 === 1)
    //assert(ranked.tail.tail.tail.tail.tail.tail.head._1 === "Occam") //Languages not contained in any articles are not added to the RDD
    //assert(ranked.tail.tail.tail.tail.tail.tail.head._2 === 0)
  }

  test("'rankLangsReduceByKey' should work for a simple RDD with four elements") {
    assert(initializeWikipediaRanking(), " -- did you fill in all the values in WikipediaRanking (conf, sc, wikiRdd)?")
    import WikipediaRanking._
    val langs = List("Scala", "Java", "Groovy", "Haskell", "Erlang")
    val articles = List(
        WikipediaArticle("1","Groovy is pretty interesting, and so is Erlang"),
        WikipediaArticle("2","Scala and Java run on the JVM"),
        WikipediaArticle("3","Scala is not purely functional"),
        WikipediaArticle("4","The cool kids like Haskell more than Java. The ones who want to work and make money write Java and leave Haskell to " +
                             "rot in the annals of other functional programming attempts"),
        WikipediaArticle("5","Java is for enterprise developers")
      )
    val rdd = sc.parallelize(articles)
    val ranked = rankLangsReduceByKey(langs, rdd)
    val res = (ranked.head._1 == "Java")
    assert(res)
  }


}
