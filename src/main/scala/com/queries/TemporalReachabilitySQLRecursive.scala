package com.queries

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.sparkconfiguration.SparkHandler._

import scala.collection.mutable

object TemporalReachabilitySQLRecursive {

  def main(args: Array[String]): Unit = {
    val dbName = if (args.length > 0) args(0) else "graph_db"
    val startEmail = if (args.length > 1) args(1) else "bob.ambrocik@enron.com"
    val startTimeStr = if (args.length > 2) args(2) else "2001-10-05 00:00:00"
    val windowHours = if (args.length > 3) args(3).toInt else 100

    val spark = spConfig("Temporal Reachability SQL WITH RECURSIVE")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    import spark.implicits._

    try {
      val person = spark.table(s"$dbName.person")
      val edgesTable = spark.table(s"$dbName.edges")

      val startNodeId = person.filter($"email" === startEmail).select($"node_id").as[Long].collect().head

      // Create temporary views so Spark SQL can query them directly
      person.createOrReplaceTempView("person")
      edgesTable.createOrReplaceTempView("edges")

      // -----------------------------------------------------------------------
      // Pure Spark SQL Execution using WITH RECURSIVE
      // -----------------------------------------------------------------------
      def ReachabilitySQL(): Map[Long, Long] = {
        
        // Pure Spark SQL WITH RECURSIVE query string
        val recursiveSqlQuery =
          s"""
             |WITH RECURSIVE temporal_reachability(node_id, arrival_time) AS (
             |    -- Base Term: Anchor seed node and starting timestamp
             |    SELECT 
             |        ${startNodeId}L AS node_id, 
             |        TIMESTAMP '$startTimeStr' AS arrival_time
             |    
             |    UNION
             |    
             |    -- Recursive Term: Join with temporal constraint (edge >= previous arrival)
             |    SELECT 
             |        e.dst AS node_id, 
             |        e.sent_at AS arrival_time
             |    FROM 
             |        temporal_reachability r
             |    JOIN 
             |        edges e 
             |      ON r.node_id = e.src
             |     AND e.sent_at >= r.arrival_time
             |     AND e.sent_at >= TIMESTAMP '$startTimeStr'
             |     AND e.sent_at <= TIMESTAMP '$startTimeStr' + INTERVAL $windowHours HOURS
             |)
             |-- Aggregation to find the earliest arrival time per reached node
             |SELECT 
             |    node_id, 
             |    CAST(UNIX_TIMESTAMP(MIN(arrival_time)) * 1000 AS BIGINT) AS arrival_millis
             |FROM 
             |    temporal_reachability
             |GROUP BY 
             |    node_id
           """.stripMargin

        val sqlResultDF = spark.sql(recursiveSqlQuery)

        // Collect results back to driver as Map[Long, Long]
        sqlResultDF
          .as[(Long, Long)]
          .collect()
          .toMap
      }

      def buildQuery(): Map[Long, Long] = {
        val start = System.nanoTime()
        val result = ReachabilitySQL()
        val elapsed = (System.nanoTime() - start) / 1e9
        println(f"Run: ${elapsed}%.3f seconds")
        result
      }

      val numRuns = 5
      val timings = mutable.ArrayBuffer[Double]()
      var lastResult: Map[Long, Long] = Map.empty

      for (i <- 1 to numRuns) {
        val start = System.nanoTime()
        lastResult = buildQuery()
        val elapsed = (System.nanoTime() - start) / 1e9
        timings += elapsed
        println(f"Run $i: $elapsed%.3f seconds (${lastResult.size} reachable nodes)")
      }

      println(f"\nCold (run 1):  ${timings.head}%.3f s")
      println(f"Warm (avg runs 2-$numRuns): ${timings.tail.sum / timings.tail.size}%.3f s")

      println("\nReachable people:")
      val resultDf = lastResult.toSeq.toDF("node_id", "arrival_millis")
        .withColumn("arrival_time", (col("arrival_millis") / 1000).cast("timestamp"))

      resultDf
        .join(person, "node_id")
        .select($"email", $"arrival_time")
        .orderBy("arrival_time")
        .show(10, truncate = false)

    } catch {
      case e: Exception =>
        println(s"Error running query: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }
}