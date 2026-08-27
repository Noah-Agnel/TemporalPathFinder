package com.sparkmultigraph

import com.sparkconfiguration.SparkHandler._

object ExportToCsv {
  def main(args: Array[String]): Unit = {
    val dbName = if (args.length > 0) args(0) else "graph_db"
    val outDir = if (args.length > 1) args(1) else "neo4j_import"

    val spark = spConfig("Export to CSV")

    try {
      spark.table(s"$dbName.person")
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(s"$outDir/person")

      spark.table(s"$dbName.edges")
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(s"$outDir/edges")

      println(s"Exported person and edges to $outDir")
    } catch {
      case e: Exception =>
        println(s"Error exporting: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }
}