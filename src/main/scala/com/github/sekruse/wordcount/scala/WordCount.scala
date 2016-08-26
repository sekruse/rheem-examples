package com.github.sekruse.wordcount.scala


import org.qcri.rheem.api._
import org.qcri.rheem.core.api.{Configuration, RheemContext}
import org.qcri.rheem.core.optimizer.ProbabilisticDoubleInterval
import org.qcri.rheem.core.plugin.Plugin
import org.qcri.rheem.java.Java
import org.qcri.rheem.spark.Spark

/**
  * This is app counts words in a file using Rheem via its Scala API.
  */
class WordCount(configuration: Configuration, plugin: Plugin*) {

  /**
    * Run the word count over a given file.
    *
    * @param inputUrl     URL to the file
    * @param wordsPerLine optional estimate of how many words there are in each line
    * @return the counted words
    */
  def apply(inputUrl: String,
            wordsPerLine: ProbabilisticDoubleInterval = new ProbabilisticDoubleInterval(100, 10000, .8d)) = {
    val rheemCtx = new RheemContext(configuration)
    plugin.foreach(rheemCtx.register)
    val planBuilder = new PlanBuilder(rheemCtx)

    planBuilder
      // Do some set up.
      .withJobName(s"WordCount ($inputUrl)")
      .withUdfJarsOf(this.getClass)

      // Read the text file.
      .readTextFile(inputUrl).withName("Load file")

      // Split each line by non-word characters.
      .flatMap(_.split("\\W+"), selectivity = wordsPerLine).withName("Split words")

      // Filter empty tokens.
      .filter(_.nonEmpty, selectivity = 0.99).withName("Filter empty words")

      // Attach counter to each word.
      .map(word => (word.toLowerCase, 1)).withName("To lower case, add counter")

      // Sum up counters for every word.
      .reduceByKey(_._1, (c1, c2) => (c1._1, c1._2 + c2._2)).withName("Add counters")
      .withCardinalityEstimator((in: Long) => math.round(in * 0.01))

      // Execute the plan and collect the results.
      .collect()
  }

}

/**
  * Companion object for [[WordCount]].
  */
object WordCount {

  def main(args: Array[String]) {
    // Parse args.
    if (args.isEmpty) {
      println("Usage: <main class> <plugin(,plugin)*> <input file> [<words per line a..b>]")
      sys.exit(1)
    }

    // Parse parameters.
    val plugins = parsePlugins(args(0))
    val inputFile = args(1)
    val wordsPerLine = if (args.length >= 3) parseWordsPerLine(args(2)) else null

    // Set up our wordcount app.
    val configuration = new Configuration
    val wordCount = new WordCount(configuration, plugins: _*)

    val words = wordCount(inputFile, wordsPerLine)

    // Print results.
    println(s"Found ${words.size} words:")
    words.take(10).foreach(wc => println(s"${wc._2}x ${wc._1}"))
    if (words.size > 10) print(s"${words.size - 10} more...")
  }

  /**
    * Parse a comma-separated list of plugins.
    *
    * @param arg the list
    * @return the [[Plugin]]s
    */
  def parsePlugins(arg: String) = arg.split(",").map {
    case "java" => Java.basicPlugin
    case "spark" => Spark.basicPlugin
    case other: String => sys.error(s"Unknown plugin: $other")
  }

  /**
    * Parse an interval string shaped as `<lower>..<upper>`.
    *
    * @param arg the string
    * @return the interval
    */
  def parseWordsPerLine(arg: String): ProbabilisticDoubleInterval = {
    val Array(low, high) = arg.split("""\.\.""").map(_.toDouble)
    new ProbabilisticDoubleInterval(low, high, 0.8)
  }

}
