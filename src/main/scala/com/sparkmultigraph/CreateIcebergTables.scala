package com.sparkmultigraph
import  org.apache.spark.sql.SparkSession
import  com.sparkconfiguration.SparkHandler._


object CreateIcebergTables {
  // Main function
  def main(args: Array[String]): Unit = {

    // Get database name from command line arguments
    val dbName = if (args.length > 0) args(0) else "graph_db"

    println(s"Creating Iceberg tables in database: $dbName")

    // Create Spark Session with Iceberg support
    val spark = spConfig("Iceberg Tables Creation")

    try {
      // Create database if it doesn't exist
      spark.sql(s"CREATE DATABASE IF NOT EXISTS $dbName")
      spark.sql(s"USE $dbName")

      println(s"Successfully created/switched to database: $dbName")

      // Create the tables
      createPersonTable(spark, dbName)
      createEdgesTable(spark, dbName)

      // Verify tables were created
      println(s"Successfully created tables in $dbName:")
      spark.sql(s"SHOW TABLES IN $dbName").show()

    } catch {
      case e: Exception =>
        println(s"Error creating tables: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }

  private def createPersonTable(spark: SparkSession, dbName: String): Unit = {
    println("Creating person table...")
    spark.sql(
      s"""
        CREATE TABLE IF NOT EXISTS $dbName.person (
          node_id     BIGINT,
          email       STRING,
          created_at  TIMESTAMP
        ) USING ICEBERG
        PARTITIONED BY (bucket(16, node_id))
      """)
  }

  private def createEdgesTable(spark: SparkSession, dbName: String): Unit = {
    println("Creating edges table...")
    spark.sql(
      s"""
        CREATE TABLE IF NOT EXISTS $dbName.edges (
          edge_id     STRING,
          src         BIGINT,
          dst         BIGINT,
          edge_type   STRING,
          sent_at     TIMESTAMP,
          created_at  TIMESTAMP
        ) USING ICEBERG
        PARTITIONED BY (bucket(16, src))
      """)
    spark.sql(
      s"""
        ALTER TABLE $dbName.edges
        WRITE ORDERED BY src, sent_at
      """)
  }
}
