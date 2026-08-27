package com.networkloading
import  org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import com.sparkconfiguration.SparkHandler._


object NetworkLoading {    
    def main(args: Array[String]): Unit = {
       val spark  = spConfig("Network Loading")
       
       // Examples of reading files from S3/MinIO buckets
       // Read JSON file from bucket
       val jsonDF = spark.read
         .option("multiline","true")
         .json("s3a://terrorismnetworkfile/path_1_city_node_static_props_1.json")
         .cache()
       
       println(jsonDF.show())     
       spark.stop()
    }
}
