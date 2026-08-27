package com.sparkmultigraph

import  org.apache.spark.sql.SparkSession
import  org.apache.spark.sql.Dataset
import  org.apache.spark.sql.Row
import  org.apache.spark.sql.functions._


object TablesPopulationHandler {
    // ========================================================================================================================
    // PERSON TABLE POPULATION
    // ========================================================================================================================
    /**
     * Flattens the raw person node JSON DataFrame into the wide `person` table shape.
     *
     * Input schema (from enron_json.py's static_nodes_json_creation):
     *   node_id: long, labels: array<string>, static_props: struct<email: string>, is_active: boolean
     *
     * @param personDS the raw person Dataset[Row] as read from MinIO (nodesDF("static")("person_static_props"))
     * @return Dataset[Row] with columns: node_id, email, created_at
    **/
    def personTablePopulation(personDS: Dataset[Row]): Dataset[Row] = {
        personDS
            .select(
                col("node_id").cast("bigint").as("node_id"),
                col("static_props.email").as("email")
            )
            .withColumn("created_at", current_timestamp())
    }

    // ========================================================================================================================
    // EDGES TABLE POPULATION
    // ========================================================================================================================
    /**
     * Flattens the raw edges JSON DataFrame into the wide `edges` table shape.
     *
     * Input schema (from enron_json.py's static_edges_json_creation):
     *   edge_id: string, source_id: long, target_id: long, edge_type: string,
     *   static_props: struct<timestamp: string>
     *
     * @param edgesDS the raw edges Dataset[Row] as read from MinIO (edgesDF("static")("edges_static_props"))
     * @return Dataset[Row] with columns: edge_id, src, dst, edge_type, sent_at, created_at
    **/
    def edgesTablePopulation(edgesDS: Dataset[Row]): Dataset[Row] = {
        edgesDS
            .where(col("source_id") !== col("target_id"))   // defensive; Python layer already filters self-loops
            .select(
                col("edge_id"),
                col("source_id").cast("bigint").as("src"),
                col("target_id").cast("bigint").as("dst"),
                col("edge_type"),
                col("static_props.timestamp").cast("timestamp").as("sent_at")
            )
            .withColumn("created_at", current_timestamp())
    }
}