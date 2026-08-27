package com.sparkmultigraph
import  org.apache.spark.sql.SparkSession
import  com.sparkconfiguration.SparkHandler._
import  com.sparkmultigraph.TablesPopulationHandler._
import  com.sparkmultigraph.ReaderWriterHandler._


object DataInsertionInTable {
    // ========================================================================================================================
    // MAIN FUNCTION
    // ========================================================================================================================
    def main(args: Array[String]): Unit = {
        if (args.length < 2){
            System.err.println("You need to provide two arguments: <database_name> <file_warehouse_name>")
            System.exit(1)
        }

        // Get database name from command line arguments
        val dbName            = args(0)
        val fileWarehouseName = args(1)

        // Create Spark Session with Iceberg support
        val spark = spConfig("Data Insertion in Table")

        try {
            // Nodes and Edges file paths from MinIO
            val filesMap = pathsReadingFromMinio(spark, fileWarehouseName)

            // Nodes and Edges dataframes: propType ("static") -> mapKey -> DataFrame
            val nodesDF = dataframesCreation(spark, filesMap("nodes"))
            val edgesDF = dataframesCreation(spark, filesMap("edges"))

            // mapKey derivation (see ReaderWriterHandler.dataframesCreation):
            // "path_1_person_static_props_0.json" -> "person_static_props"
            // "path_1_edges_static_props_0.json"  -> "edges_static_props"
            val rawPersonDS = nodesDF("static")("person_static_props")
            val rawEdgesDS  = edgesDF("static")("edges_static_props")

            // Flatten into wide table shapes
            val personTab = personTablePopulation(rawPersonDS)
            val edgesTab  = edgesTablePopulation(rawEdgesDS)

            // Saving tables data
            personTab
              .write
              .mode("append")
              .insertInto(s"$dbName.person")

            edgesTab
              .write
              .mode("append")
              .insertInto(s"$dbName.edges")

            println(s"Inserted ${personTab.count()} rows into $dbName.person")
            println(s"Inserted ${edgesTab.count()} rows into $dbName.edges")

        } catch {
            case e: Exception =>
                println(s"Error inserting data: ${e.getMessage}")
                e.printStackTrace()
        } finally {
            spark.stop()
        }
    }
}