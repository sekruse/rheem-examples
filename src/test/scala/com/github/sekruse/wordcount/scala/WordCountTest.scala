package com.github.sekruse.wordcount.scala

import org.junit.{Assert, Test}
import org.qcri.rheem.core.api
import org.qcri.rheem.core.plugin.Plugin
import org.qcri.rheem.core.util.ReflectionUtils
import org.qcri.rheem.java.Java
import org.qcri.rheem.spark.Spark

/**
  * Test suite for the [[WordCount]].
  */
class WordCountTest {

  @Test
  def shouldWorkWithJava: Unit = shouldWorkWith(Java.basicPlugin)

  @Test
  def shouldWorkWithSpark: Unit = shouldWorkWith(Spark.basicPlugin)

  @Test
  def shouldWorkWithJavaAndSpark: Unit = shouldWorkWith(Java.basicPlugin, Spark.basicPlugin)

  def shouldWorkWith(plugins: Plugin*): Unit = {
    val wordCount = new WordCount(new api.Configuration, plugins: _*)
    val inputUrl = ReflectionUtils.getResourceURL("lorem-ipsum.txt")

    val wordFrequencies = wordCount(inputUrl.toString).toMap

    // Test some samples.
    Assert.assertEquals(4, wordFrequencies("quia"))
    Assert.assertEquals(1, wordFrequencies("doloremque"))
  }

  @Test
  def shouldWorkViaMainMethod1: Unit = {
    val args = Array(
      "java,spark",
      ReflectionUtils.getResourceURL("lorem-ipsum.txt").toString,
      "100..200"
    )
    WordCount.main(args)
  }

  @Test
  def shouldWorkViaMainMethod2: Unit = {
    val args = Array(
      "java,spark",
      ReflectionUtils.getResourceURL("lorem-ipsum.txt").toString
    )
    WordCount.main(args)
  }

}
