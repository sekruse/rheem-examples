# Word count

This repository contains two implementations of word count (the "Hello, World!" for data processing) for Rheem using both the Java and Scala API.

# Installation

The app can be built with Maven.

# Usage

You can run the app via
```shell
$ java/scala ... com.github.sekruse.wordcount.java.WordCount/com.github.sekruse.wordcount.scala.WordCount <plugins> <input file> [<words per line>]
```
* `<plugins>`: comma-separated file of processing platforms to use (`java`, `spark`)
* `<input file>`: *URL* to the input file whose words should be counted
* `<words per line>`: optional estimate of words per line (`min..max`); if given, Rheem can leverage this information for optimization
