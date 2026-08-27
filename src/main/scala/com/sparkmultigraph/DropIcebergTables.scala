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