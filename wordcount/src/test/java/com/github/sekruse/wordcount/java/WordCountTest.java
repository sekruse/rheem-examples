package com.github.sekruse.wordcount.java;

import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.plugin.Plugin;
import org.qcri.rheem.core.util.ReflectionUtils;
import org.qcri.rheem.java.Java;
import org.qcri.rheem.spark.Spark;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Test suite for {@link WordCount}.
 */
public class WordCountTest {

    @Test
    public void shouldWorkWithJava() {
        this.shouldWorkWith(Java.basicPlugin());
    }

    @Test
    public void shouldWorkWithSpark() {
        this.shouldWorkWith(Spark.basicPlugin());
    }

    @Test
    public void shouldWorkWithJavaAndSpark() {
        this.shouldWorkWith(Java.basicPlugin(), Spark.basicPlugin());
    }

    private void shouldWorkWith(Plugin... plugins) {
        // Prepare.
        WordCount wordCount = new WordCount(new Configuration(), plugins);
        final String inputUrl = ReflectionUtils.getResourceURL("lorem-ipsum.txt").toString();

        // Run.
        Map<String, Integer> wordCounts = wordCount
                .execute(inputUrl)
                .stream()
                .collect(Collectors.toMap(Tuple2::getField0, Tuple2::getField1));

        // Test.
        Assert.assertEquals(wordCounts.get("quia"), Integer.valueOf(4));
        Assert.assertEquals(wordCounts.get("doloremque"), Integer.valueOf(1));
    }

    @Test
    public void shouldWorkViaMainMethod1() {
        String[] args = new String[]{
                "java,spark",
                ReflectionUtils.getResourceURL("lorem-ipsum.txt").toString(),
                "100..200"
        };
        WordCount.main(args);
    }

    @Test
    public void shouldWorkViaMainMethod2() {
        String[] args = new String[]{
                "java,spark",
                ReflectionUtils.getResourceURL("lorem-ipsum.txt").toString(),
        };
        WordCount.main(args);
    }

}
