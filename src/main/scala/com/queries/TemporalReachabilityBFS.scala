package com.queries

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import com.sparkconfiguration.SparkHandler._
import org.apache.spark.util.SizeEstimator

import scala.collection.mutable

object TemporalReachabilityBFS {

  def main(args: Array[String]): Unit = {
    val dbName = if (args.length > 0) args(0) else "graph_db"
    val startEmail = if (args.length > 1) args(1) else "bob.ambrocik@enron.com"
    val startTimeStr = if (args.length > 2) args(2) else "2001-10-05 00:00:00"
    val windowHours = if (args.length > 3) args(3).toInt else 100

    val spark = spConfig("Temporal Reachability BFS")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    import spark.implicits._

    try {
      val person = spark.table(s"$dbName.person")
      val edgesTable = spark.table(s"$dbName.edges")

      val startNodeId = person.filter($"email" === startEmail).select($"node_id").as[Long].collect().head
      val startTime = lit(startTimeStr).cast("timestamp")
      val windowEnd = expr(s"timestamp('$startTimeStr') + INTERVAL $windowHours HOURS")

      // Local (driver-side) time bounds, in millis, used by the in-memory traversal.
      // java.sql.Timestamp.valueOf is strict about "yyyy-mm-dd hh:mm:ss" (space
      // separator), so normalize ISO-style "T" separators (and trim trailing "Z")
      // before parsing, since Spark's timestamp() cast above already accepts both.
      val normalizedTimeStr = startTimeStr.replace("T", " ").stripSuffix("Z").trim
      val startMillis = java.sql.Timestamp.valueOf(normalizedTimeStr).getTime
      val windowEndMillis = startMillis + windowHours.toLong * 3600000L


      def loadAdjacency(): Map[Long, Array[(Long, Long)]] = {
        val rows = edgesTable
          .filter($"sent_at" >= startTime && $"sent_at" <= windowEnd) //only look at edges within the timeframe
          .select($"src".as[Long], $"dst".as[Long], $"sent_at".as[java.sql.Timestamp])
          .collect()

        rows
          .groupBy(_._1)
          .map { case (src, arr) =>
            src -> arr.map { case (_, dst, t) => (dst, t.getTime) }
          }
      }

      def Reachability(adj: Map[Long, Array[(Long, Long)]]): Map[Long, Long] = {
        val arrival = mutable.Map[Long, Long](startNodeId -> startMillis) //best arrival time on record
        val settled = mutable.Set.empty[Long] //once settled, we never look at the nodes outgoing edges again
        val pq = mutable.PriorityQueue.empty[(Long, Long)](Ordering.by((p: (Long, Long)) => -p._2))
        pq.enqueue((startNodeId, startMillis)) //pop next earliest node from the priority queue

        while (pq.nonEmpty) {
          val (node, t) = pq.dequeue()
          if (!settled.contains(node)) {
            settled += node
            adj.getOrElse(node, Array.empty).foreach { case (dst, edgeTime) =>
              if (edgeTime >= t && (!arrival.contains(dst) || edgeTime < arrival(dst))) {
                arrival(dst) = edgeTime
                pq.enqueue((dst, edgeTime))
              }
            }
          }
        }
        arrival.toMap
      }

      def buildQuery(): Map[Long, Long] = {
        val adj = loadAdjacency()
        val start = System.nanoTime()
        val result = Reachability(adj)
        val elapsed = (System.nanoTime() - start) / 1e9
        println(f"Run: ${elapsed}%.3f seconds")
        println(f"Estimated adjacency size: ${SizeEstimator.estimate(adj) / 1e6}%.2f MB")
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