package com.sparkmultigraph
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.hadoop.fs.{FileSystem, Path}
import scala.collection.mutable
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Column
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._


object ReaderWriterHandler {
    // ===========================================================================================================================
    // PATH READING FROM MINIO
    // ===========================================================================================================================
    /**
     * Reads file paths from a MinIO bucket and groups them into nodes and edges.
     *
     * @param spark      the SparkSession to use for filesystem access
     * @param bucketName the name of the MinIO bucket to read files from
     *
     * @return a nested mutable.Map[String, mutable.Map[String, Array[String]]] where the outer key
     *         is "nodes" or "edges", the inner key is a composite of entity type and partition number,
     *         and the value is an array of file path strings
    **/
    def pathsReadingFromMinio(
        spark     :SparkSession, 
        bucketName:String
    ): mutable.Map[String, mutable.Map[String, Array[String]]] = 
    {
        // 1.1 FILESYSTEM CONFIGURATION
        val conf = spark.sparkContext.hadoopConfiguration
        val path = new Path(bucketName)
        val fs   = FileSystem.get(path.toUri, conf)

        // 1.2 FILES NAME READING
        val filesPath = fs.listStatus(path).map(path => path.getPath)

        // 1.3 MAP CONFIGURATION
        val filesMap  = mutable.Map(
            "nodes" -> mutable.Map.empty[String, Array[String]],
            "edges" -> mutable.Map.empty[String, Array[String]]
        )

        // 1.4 MAP POPULATION
        filesPath
            .filter(fp => !fp.toString.endsWith("/warehouse") && fp.toString.endsWith(".json"))
            .foreach(filePath => {
            val filePathStr         = filePath.toString
            val fileNameComponents  = filePathStr.split("/").last.split("_")
            val partitionNumber     = fileNameComponents.last.split("\\.")(0)
            val key                 = fileNameComponents(0) + "_" + fileNameComponents(1) + "_" + partitionNumber    
            val elemType = if(filePathStr.contains("person")) "nodes" else "edges"
                
            if (!filesMap(elemType).contains(key))
                filesMap(elemType) += (key -> Array.empty)
        
            filesMap(elemType).update(key, filesMap(elemType)(key):+filePathStr)    
        })

        return filesMap
    }
    // ===========================================================================================================================

    
    // ===========================================================================================================================
    // ELEMENTS DATAFRAME CREATION
    // ===========================================================================================================================
    /**
     * Creates dataframes from a collection of file paths.
     *
     * @param spark    the SparkSession to use for dataframe creation
     * @param elements a mutable.Map[String, Array[String]] containing file paths grouped by entity type and partition number
     *
     * @return a nested mutable.Map[String, mutable.Map[String, Dataset[Row]]] where the outer key
     *         is "static" or "dynamic", the inner key is entity type, and the value is a Dataset[Row] containing the dataframe
    **/
    def dataframesCreation(
        spark   :SparkSession,
        elements: mutable.Map[String, Array[String]]
    ): mutable.Map[String, mutable.Map[String, Dataset[Row]]] = { 

        //     key_1            key_2         value
        // property_type -> element_type -> dataframe
        var elementsDF: mutable.Map[String, mutable.Map[String, Dataset[Row]]] = mutable.Map.empty
        
        elements.foreach {case (key, paths) => {
            paths.foreach(path => {
                println("Reading path: " + path)
                val pathFileName  = path.split("/").last
                val mainNameParts = pathFileName.split("_")
                
                // 1. WE ARE DEFINING THE element_type and property one
                // THE PATH IS LIKE THIS: path_1_element_type_partition.json
                // SO WE SELECT element_type 
                val mapKey        = mainNameParts.slice(2, mainNameParts.length - 1).mkString("_")
                val propType      = if (path.contains("static")) "static" else "dynamic"

                // 2. WE ADD THE PROPERTY TYPE IF NOT EXIST
                if (!elementsDF.contains(propType))
                    elementsDF += (propType -> mutable.Map.empty)
                
                // 4. DATAFRAME READING FROM MINIO
                val elementDF = spark.read.option("multiline","true").json(path)
                
                // 5. IF IT IS THE FIRST DATAFRAME, WE ASSIGN
                elementsDF(propType)(mapKey) = elementsDF(propType).get(mapKey) match {
                    case Some(existing) => existing.union(elementDF).dropDuplicates()
                    case None           => elementDF
                }
            })
        }}

        return elementsDF
    }
}