package com.queries

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import com.sparkconfiguration.SparkHandler._

import scala.collection.mutable

object TemporalReachabilityStream {

  // Simple, flat data structure representing a single edge
  case class FlatEdge(src: Long, dst: Long, timeMillis: Long)

  def main(args: Array[String]): Unit = {
    val dbName = if (args.length > 0) args(0) else "graph_db"
    val startEmail = if (args.length > 1) args(1) else "bob.ambrocik@enron.com"
    val startTimeStr = if (args.length > 2) args(2) else "2001-10-05 00:00:00"
    val windowHours = if (args.length > 3) args(3).toInt else 100

    val spark = spConfig("Temporal Reachability Chronological Stream")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    import spark.implicits._

    try {
      val person = spark.table(s"$dbName.person")
      val edgesTable = spark.table(s"$dbName.edges")

      val startNodeId = person.filter($"email" === startEmail).select($"node_id").as[Long].collect().head
      val startTime = lit(startTimeStr).cast("timestamp")
      val windowEnd = expr(s"timestamp('$startTimeStr') + INTERVAL $windowHours HOURS")

      val normalizedTimeStr = startTimeStr.replace("T", " ").stripSuffix("Z").trim
      val startMillis = java.sql.Timestamp.valueOf(normalizedTimeStr).getTime

      // =========================================================================
      // Step 1: Extract ALL edges in the time window and sort them chronologically
      // =========================================================================
      def loadSortedStream(): Array[FlatEdge] = {
        edgesTable
          .filter($"sent_at" >= startTime && $"sent_at" <= windowEnd)
          .select($"src".as[Long], $"dst".as[Long], $"sent_at".as[java.sql.Timestamp])
          .collect()
          // Convert directly into a flat array of lightweight case classes
          .map { case (src, dst, t) => FlatEdge(src, dst, t.getTime) }
          // Sort the entire graph chronologically from past to present
          .sortBy(_.timeMillis)
      }

      // =========================================================================
      // Step 2: Single Pass Stream Scan (Cursorless Pointer Traversal)
      // =========================================================================
      def streamReachability(globalEdges: Array[FlatEdge]): Map[Long, Long] = {
        // Maps reachable Node ID -> Earliest Arrival Time
        val arrival = mutable.Map[Long, Long](startNodeId -> startMillis)
        
        var i = 0
        val totalEdges = globalEdges.length

        // Scan every single edge exactly once in a straight linear pass
        while (i < totalEdges) {
          val edge = globalEdges(i)

          // Condition: The source must be reachable, AND the edge time must happen
          // AFTER (or at) the source node's recorded arrival time.
          if (arrival.contains(edge.src) && edge.timeMillis >= arrival(edge.src)) {
            
            // Condition: The destination hasn't been reached yet, OR this current
            // edge offers a faster/earlier path to them.
            if (!arrival.contains(edge.dst) || edge.timeMillis < arrival(edge.dst)) {
              arrival(edge.dst) = edge.timeMillis
            }
          }
          i += 1
        }
        arrival.toMap
      }

      def buildQuery(): Map[Long, Long] = {
        val globalStream = loadSortedStream()
        val result = streamReachability(globalStream)
        
        result
      }

      // Benchmarking Execution Loop
      val numRuns = 5
      val timings = mutable.ArrayBuffer[Double]()
      var lastResult: Map[Long, Long] = Map.empty

      for (i <- 1 to numRuns) {
        val start = System.nanoTime()
        lastResult = buildQuery()
        val elapsed = (System.nanoTime() - start) / 1e9
        timings += elapsed
        println(f"Run $i (Extract + Sort + Scan): $elapsed%.3f seconds (${lastResult.size} reachable nodes)")
      }

      println(f"\nCold (run 1):  ${timings.head}%.3f s")
      println(f"Warm (avg runs 2-$numRuns): ${timings.tail.sum / timings.tail.size}%.3f s")

      // Display sample output
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