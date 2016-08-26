package com.github.sekruse.wordcount.java;

import org.qcri.rheem.api.JavaPlanBuilder;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.optimizer.ProbabilisticDoubleInterval;
import org.qcri.rheem.core.optimizer.cardinality.DefaultCardinalityEstimator;
import org.qcri.rheem.core.plugin.Plugin;
import org.qcri.rheem.java.Java;
import org.qcri.rheem.spark.Spark;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

/**
 * This is app counts words in a file using Rheem via its Scala API
 */
public class WordCount {

    /**
     * {@link Configuration} to control Rheem.
     */
    private final Configuration configuration;

    /**
     * {@link Plugin}s that Rheem may use.
     */
    private final Collection<Plugin> plugins;

    /**
     * Creates a new instance of this app.
     *
     * @param configuration the {@link Configuration} to use for Rheem
     * @param plugins       the {@link Plugin}s Rheem may use
     */
    public WordCount(Configuration configuration, Plugin... plugins) {
        this.configuration = configuration;
        this.plugins = Arrays.asList(plugins);
    }

    /**
     * Execute the word count.
     *
     * @param inputUrl URL to the input file
     * @return the word counts
     */
    public Collection<Tuple2<String, Integer>> execute(String inputUrl) {
        return this.execute(inputUrl, new ProbabilisticDoubleInterval(100, 10000, 0.8));
    }

    /**
     * Execute the word count.
     *
     * @param inputUrl     URL to the input file
     * @param wordsPerLine alleged words per line in the input file
     * @return the word counts
     */
    public Collection<Tuple2<String, Integer>> execute(String inputUrl, ProbabilisticDoubleInterval wordsPerLine) {
        // Prepare the Rheem context.
        RheemContext rheemContext = new RheemContext(this.configuration);
        this.plugins.forEach(rheemContext::register);

        // Start building the RheemPlan.
        return new JavaPlanBuilder(rheemContext)
                .withJobName(String.format("WordCount (%s)", inputUrl))
                .withUdfJarOf(this.getClass())

                // Read the text file.
                .readTextFile(inputUrl).withName("Load file")


                // Split each line by non-word characters.
                .flatMap(line -> Arrays.asList(line.split("\\W+")))
                .withSelectivity(wordsPerLine.getLowerEstimate(), wordsPerLine.getUpperEstimate(), wordsPerLine.getCorrectnessProbability())
                .withName("Split words")

                // Filter empty tokens.
                .filter(token -> !token.isEmpty())
                .withSelectivity(0.99, 0.99, 0.99)
                .withName("Filter empty words")

                // Attach counter to each word.
                .map(word -> new Tuple2<>(word.toLowerCase(), 1)).withName("To lower case, add counter")

                // Sum up counters for every word.
                .reduceByKey(
                        Tuple2::getField0,
                        (t1, t2) -> new Tuple2<>(t1.getField0(), t1.getField1() + t2.getField1())
                )
                .withCardinalityEstimator(new DefaultCardinalityEstimator(0.9, 1, false, in -> Math.round(0.01 * in[0])))
                .withName("Add counters")

                // Execute the plan and collect the results.
                .collect();
    }

    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("Usage: <main class> <plugin(,plugin)*> <input file> [<words per line a..b>]");
            System.exit(1);
        }
        Plugin[] plugins = parsePlugins(args[0]);
        String inputUrl = args[1];
        ProbabilisticDoubleInterval wordsPerLine = args.length > 2 ? parseWordsPerLine(args[2]) : null;

        // Set up our wordcount app.
        Configuration configuration = new Configuration();
        WordCount wordCount = new WordCount(configuration, plugins);

        // Run the wordcount.
        Collection<Tuple2<String, Integer>> wordCounts = wordsPerLine == null ?
                wordCount.execute(inputUrl) :
                wordCount.execute(inputUrl, wordsPerLine);

        // Print the results.
        System.out.printf("Found %d words:\n", wordCounts.size());
        int numPrintedWords = 0;
        for (Tuple2<String, Integer> count : wordCounts) {
            if (++numPrintedWords >= 10) break;
            System.out.printf("%dx %s\n", count.getField1(), count.getField0());
        }
        if (numPrintedWords < wordCounts.size()) {
            System.out.printf("%d more...\n", wordCounts.size() - numPrintedWords);
        }
    }


    /**
     * Parse a comma-separated list of plugins.
     *
     * @param arg the list
     * @return the {@link Plugin}s
     */
    private static Plugin[] parsePlugins(String arg) {
        return Arrays.stream(arg.split(","))
                .map(token -> {
                    switch (token) {
                        case "java":
                            return Java.basicPlugin();
                        case "spark":
                            return Spark.basicPlugin();
                        default:
                            throw new IllegalArgumentException("Unknown platform: " + token);
                    }
                })
                .collect(Collectors.toList()).toArray(new Plugin[0]);
    }

    /**
     * Parse an interval string shaped as `<lower>..<upper>`.
     *
     * @param arg the string
     * @return the interval
     */
    private static ProbabilisticDoubleInterval parseWordsPerLine(String arg) {
        String[] tokens = arg.split("\\.\\.");
        return new ProbabilisticDoubleInterval(Long.parseLong(tokens[0]), Long.parseLong(tokens[1]), 0.8);
    }

}
