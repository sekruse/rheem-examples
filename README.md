# Rheem examples

This repository contains several simple example apps for [Rheem](http://da.qcri.org/rheem/). All of them are written in Scala and/or Java and are either IPython notebooks (with the jupyter-scala kernel) or can be built with Maven. Please find details about the different apps in the following.

## Wordcount

Wordcount counts the different distinct words occurring in a text and is basically the "Hello, World!" for data processing. We provide two implementations of it using both Rheem's Java and Scala API.

**Usage.** You can run the app via
```shell
$ java/scala ... com.github.sekruse.wordcount.java.WordCount/com.github.sekruse.wordcount.scala.WordCount <plugins> <input file> [<words per line>]
```
* `<plugins>`: comma-separated file of plugins (~platforms) to use (`java`, `spark`)
* `<input file>`: *URL* to the input file whose words should be counted
* `<words per line>`: optional estimate of words per line (`min..max`); if given, Rheem can leverage this information for optimization

## SINDY

This app is an implementation of the [SINDY](https://hpi.de/fileadmin/user_upload/fachgebiete/naumann/publications/2015/Scaling_out_the_discovery_of_INDs-CR.pdf) algorithm to find inclusion dependencies in relational databases. It employs Rheem for platform independence and cross-platform query processing, respectively. In the current version, the data is expected to reside in a SQLite3 database.

**Usage.** You can run the app via
```shell
$ java/scala com.github.sekruse.sindy.Sindy <plugins> <JDBC URL> <schema file> [<tables ...>]
```
* `<plugins>`: comma-separated list of plugins (`java`, `spark`, `sqlite3`) to use for processing
* `<JDBC URL>`: JDBC URL to the database, i.e., something like `jdbc:sqlite:...`
* `<schema file>`: path to schema file that describes the schema of the input database; for each table of the database, there must be a line in the file with the pattern `table[column1,column2,...]`
* `<tables ...>`: specification of tables/columns to be considered; each token must adhere to the pattern `table[columnX,columnY,...]` or `table[*]`, respectively, to consider all columns in a table; if left out, all tables/columns will be considered

## PageRank

This app consumes an RDF triple file, constructs a graph from it, and finally runs a PageRank on that graph. It uses Rheem to easily bring together two different data processing tasks, namely preprocessing and graph analytics, and also makes use of Rheem's `PageRankOperator`.


**Usage.** You can run the app via
```shell
$ java/scala com.github.sekruse.pagerank.PageRank <plugins> <input file> <#iterations>
```
* `<plugins>`: comma-separated list of plugins (`basic-graph`, `java`, `java-graph`, `java-conversions`, `spark`, `spark-graph`, `graphchi`) to use for processing
* `<input file>`: *URL* to the NTriple RDF file
* `<#iterations>`: number of PageRank iterations to perform