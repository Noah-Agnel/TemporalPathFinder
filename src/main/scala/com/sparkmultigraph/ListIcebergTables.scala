package com.sparkmultigraph
import  org.apache.spark.sql.SparkSession
import  com.sparkconfiguration.SparkHandler._


object ListIcebergTables { 
  // Main function
  def main(args: Array[String]): Unit = {
    
    // Get database name from command line arguments
    val dbName = if (args.length > 0) args(0) else "graph_db"
    
    println(s"Listing Iceberg tables in database: $dbName")
    
    // Create Spark Session with Iceberg support
    val spark  = spConfig("List Iceberg Tables")


    try {
      // NOTE: We do NOT gate on SHOW DATABASES / SHOW TABLES IN $dbName here.
      // With SparkSessionCatalog + Hadoop-type catalog, namespace listing is
      // known to be unreliable -- it can report a database/table as absent
      // even though it exists on disk and is perfectly queryable. Relying on
      // that check previously caused this script to bail out on a database
      // that actually had live data in it. Instead we query the known table
      // names directly and let a real failure (not a listing false-negative)
      // surface as an exception.

      val tableNames = Array("person", "edges")

      tableNames.foreach { tableName =>
        println(s"\n=== Table: $dbName.$tableName ===")

        try {
          // Show table schema
          println("Schema:")
          spark.sql(s"DESCRIBE $dbName.$tableName").show()

          // Show table properties (if supported)
          println("Table Properties:")
          spark.sql(s"SHOW TBLPROPERTIES $dbName.$tableName").show()

          // Show row count
          val count = spark.sql(s"SELECT COUNT(*) as row_count FROM $dbName.$tableName").collect()
          println(s"Row count: ${count(0).getLong(0)}")

          // Peek at a few rows
          println("Sample rows:")
          spark.sql(s"SELECT * FROM $dbName.$tableName LIMIT 10").show(truncate = false)

        }
        catch {
          case e: Exception =>
            println(s"Error getting details for table $dbName.$tableName: ${e.getMessage}")
        }
      }

    } 
    catch {
       case e: Exception =>
         println(s"Error listing tables: ${e.getMessage}")
         e.printStackTrace()
    } 
    finally {
      spark.stop()
    }
  }
}