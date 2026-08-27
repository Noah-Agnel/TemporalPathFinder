package com.sparkmultigraph
import  org.apache.spark.sql.SparkSession
import  com.sparkconfiguration.SparkHandler._



object DropIcebergTables {
  //Main function
  def main(args: Array[String]): Unit = {
    
    // Get database name from command line arguments
    val dbName = if (args.length > 0) args(0) else "graph_db"
    
    println(s"Dropping Iceberg tables in database: $dbName")
    
    // Create Spark Session with Iceberg support
    val spark  = spConfig("Drop Iceberg Tables")

    try {
      // NOTE: We deliberately do NOT gate on SHOW DATABASES / SHOW TABLES here.
      // With SparkSessionCatalog + Hadoop-type catalog, namespace listing is
      // known to be unreliable (it can report a database/table as absent even
      // though it exists on disk and is perfectly queryable/writable). Relying
      // on that check caused this script to bail out early on a database that
      // actually existed. DROP TABLE IF EXISTS is safe to call unconditionally,
      // so we just attempt the drops directly instead.

      val tableNames = Array(
        "person",
        "edges"
      )

      tableNames.foreach { tableName =>
        try {
          println(s"Dropping table $dbName.$tableName...")
          spark.sql(s"DROP TABLE IF EXISTS $dbName.$tableName")
          println(s"Successfully dropped (or confirmed absent) $tableName")
        }
        catch {
          case e: Exception =>
            println(s"Error dropping table $tableName: ${e.getMessage}")
        }
      }

      // Skipping the "drop database if empty" step for the same reason --
      // SHOW TABLES may under-report contents. If you want the database gone
      // too, drop it explicitly once you've confirmed (e.g. via a direct
      // listing of the warehouse directory on disk) that it's actually empty.

    } 
    catch {
      case e: Exception =>
        println(s"Error dropping tables: ${e.getMessage}")
        e.printStackTrace()
    } 
    finally {
      spark.stop()
    }
  }
}