package com.github.sekruse.sindy

import java.util

import com.github.sekruse.sindy.Sindy._
import org.qcri.rheem.api.{PlanBuilder, _}
import org.qcri.rheem.basic.data.Record
import org.qcri.rheem.core.api.{Configuration, RheemContext}
import org.qcri.rheem.core.function.FunctionDescriptor.{SerializableBinaryOperator, SerializableFunction}
import org.qcri.rheem.core.plugin.Plugin
import org.qcri.rheem.java.Java
import org.qcri.rheem.spark.Spark
import org.qcri.rheem.sqlite3.Sqlite3
import org.qcri.rheem.sqlite3.operators.Sqlite3TableSource

import scala.collection.mutable

/**
  * This is a Rheem-based implementation of the SINDY algorithm.
  */
class Sindy(configuration: Configuration, plugins: Plugin*) {

  /**
    * Execute the SINDY algorithm.
    *
    * @param jdbcUrl JDBC URL to the SQLite3 database
    * @param schema  mapping of all tables to their columns
    * @param tables  specification of which tables/columns to consider
    * @return the INDs
    */
  def apply(jdbcUrl: String, schema: Map[String, Seq[String]], tables: (String, Seq[String])*) = {

    // Prepare the RheemContext.
    val rheemContext = new RheemContext(configuration)
    plugins.foreach(rheemContext.register)
    rheemContext.getConfiguration.setProperty("rheem.sqlite3.jdbc.url", jdbcUrl)
    val planBuilder = new PlanBuilder(rheemContext)

    // Create cells from the various tables.
    var offset = 0
    val columnsById = mutable.Map[Int, String]()
    val allCells = tables
      .map { case (table, columns) =>
        // Handle the special case where columns == "*".
        var resolvedColumns = if (columns == Seq("*")) schema(table) else columns

        // Read the cells from the specified table/columns.
        var records = planBuilder.readTable(new Sqlite3TableSource(table, schema(table): _*)).withName(s"Load $table")

        // If requested, project to the relevant fields.
        if (resolvedColumns.toSet != schema(table).toSet)
          records = records.projectRecords(resolvedColumns).withName(s"Project $table")

        // Create the cells, finally.
        val cells = records.flatMapJava(new CellCreator(offset), selectivity = resolvedColumns.size.toDouble).withName(s"Create cells for $table")

        // Maintain some helper data structures.
        resolvedColumns.zipWithIndex.foreach { case (column, index) => columnsById(offset + index) = s"$table[$column]" }
        offset += resolvedColumns.size

        cells
      }
      .reduce(_ union _)

    // Do the rest of the SINDY logic on the cells.
    val rawInds = allCells
      .map(cell => (cell._1, Array(cell._2))).withName("Prepare cell merging")
      .reduceByKeyJava(toSerializableFunction(_._1), new CellMerger).withName("Merge cells")
      .flatMapJava(new IndCandidateGenerator).withName("Generate IND candidate sets")
      .reduceByKeyJava(toSerializableFunction(_._1), new IndCandidateMerger).withName("Merge IND candidate sets")
      .filter(_._2.length > 0).withName("Filter empty candidate sets")
      .collect()

    // Make the results readable.
    rawInds.map {
      case (dep, refs) => (s"${columnsById(dep)}", refs.map(columnsById).toSeq)
    }
  }

}

/**
  * Companion object for [[Sindy]].
  */
object Sindy {

  def main(args: Array[String]): Unit = {
    // Parse parameters.
    if (args.isEmpty) {
      sys.error("Usage: <main class> <plugin>(,<plugin>)* <JDBC URL> <schema definition> <table name>[<column>(,<column>)*]*")
      sys.exit(1)
    }
    val plugins = parsePlugins(args(0))
    val inputUrl = args(1)
    val schema = loadSchema(args(2))
    val tables = if (args.length > 3) args.slice(3, args.length).map(parseTable).toSeq else schema.toSeq

    // Prepare SINDY.
    val configuration = new Configuration
    val sindy = new Sindy(configuration, plugins: _*)

    // Run SINDY.
    val inds = sindy(inputUrl, schema, tables: _*)

    // Print the result.
    inds.foreach {
      case (dep, refs) => println(s"$dep is in included in: ${refs.mkString(", ")}")
    }
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
    case "sqlite3" => Sqlite3.plugin
    case other: String => sys.error(s"Unknown plugin: $other")
  }

  private val tableRegex = """(\w+)\[((?:\w+(?:,\w+)*)|(?:\*))\]""".r

  /**
    * Parse a table/column argument of pattern `table[column,column,...]`.
    *
    * @param arg the pattern
    * @return the table specification
    */
  def parseTable(arg: String) = arg match {
    case tableRegex(table, columns) => (table, columns.split(',').toSeq)
    case other: String => sys.error(s"Illegal table specifier: $other")
  }

  /**
    * Load the schema for the database.
    *
    * @param file path to the schema file
    * @return a mapping from table names to their columns
    */
  def loadSchema(file: String) = scala.io.Source.fromFile(file).getLines()
    .map(parseTable)
    .toMap

  /**
    * UDF to create cells from a [[Record]].
    *
    * @param offset the column ID offset for the input [[Record]]s
    */
  class CellCreator(val offset: Int) extends SerializableFunction[Record, java.lang.Iterable[(String, Int)]] {

    override def apply(record: Record): java.lang.Iterable[(String, Int)] = {
      val cells = new util.ArrayList[(String, Int)](record.size)
      var columnId = offset
      for (index <- 0 until record.size) {
        cells.add((record.getString(index), columnId))
        columnId += 1
      }
      cells
    }
  }

  /**
    * UDF to merge the column IDs of two cells.
    */
  class CellMerger extends SerializableBinaryOperator[(String, Array[Int])] {

    lazy val merger = mutable.Set[Int]()

    override def apply(cell1: (String, Array[Int]), cell2: (String, Array[Int])): (String, Array[Int]) = {
      merger.clear()
      for (columnId <- cell1._2) merger += columnId
      for (columnId <- cell2._2) merger += columnId
      (cell1._1, merger.toArray)
    }
  }

  /**
    * UDF to create IND candidates from a cell group.
    */
  class IndCandidateGenerator extends SerializableFunction[(String, Array[Int]), java.lang.Iterable[(Int, Array[Int])]] {

    override def apply(cellGroup: (String, Array[Int])): java.lang.Iterable[(Int, Array[Int])] = {
      val columnIds = cellGroup._2
      val result = new util.ArrayList[(Int, Array[Int])](columnIds.length)
      for (i <- columnIds.indices) {
        val refColumnIds = new Array[Int](columnIds.length - 1)
        java.lang.System.arraycopy(columnIds, 0, refColumnIds, 0, i)
        java.lang.System.arraycopy(columnIds, i + 1, refColumnIds, i, refColumnIds.length - i)
        result.add((columnIds(i), refColumnIds))
      }
      result
    }
  }

  /**
    * UDF to merge two IND candidates.
    */
  class IndCandidateMerger extends SerializableBinaryOperator[(Int, Array[Int])] {

    lazy val merger = mutable.Set[Int]()

    override def apply(indc1: (Int, Array[Int]), indc2: (Int, Array[Int])): (Int, Array[Int]) = {
      merger.clear()
      for (columnId <- indc1._2) merger += columnId
      (indc1._1, indc2._2.filter(merger.contains))
    }

  }


}
