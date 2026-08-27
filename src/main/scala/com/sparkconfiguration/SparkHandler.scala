package com.sparkconfiguration
import  org.apache.spark.sql.SparkSession


object SparkHandler {
    // ========================================================================================================================
    // SPARK CONFIGURATION
    // ========================================================================================================================
    def spConfig(appName: String): SparkSession = {
        val spark = SparkSession.builder()
        .appName(appName)
        .master("local[*]")

        // --- Driver memory / result size: raised because collecting the full
        // one-hop + two-hop adjacency to the driver for in-memory BFS can
        // exceed the 1g defaults on larger two-hop tables ---
        .config("spark.driver.memory", "6g")
        .config("spark.driver.maxResultSize", "4g")

        // --- Iceberg catalog: Hadoop-type catalog backed by a warehouse path on local disk ---
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
        .config("spark.sql.catalog.spark_catalog.type", "hadoop")
        .config("spark.sql.catalog.spark_catalog.warehouse", "file:///Users/noah/Desktop/temporalPathFinder/warehouse")

        .getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        
        return spark
    }
}