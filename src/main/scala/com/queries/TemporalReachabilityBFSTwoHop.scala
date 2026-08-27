package com.sparkmultigraph

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import com.sparkconfiguration.SparkHandler._

import scala.collection.mutable

object TemporalReachabilityBFSTwoHop {

  // Unified edge shape: for one-hop edges start == end (a single sent_at).
  // For two-hop compound edges, start = t1 (departure from n1) and
  // end = t2 (arrival at n3 via n2) -- the intermediate hop is folded away.
  case class TemporalEdge(src: Long, start: java.sql.Timestamp, end: java.sql.Timestamp, dst: Long)

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
      val twoHopTable = spark.table(s"$dbName.two_hop_index")

      val startNodeId = person.filter($"email" === startEmail).select($"node_id").as[Long].collect().head
      val startTime = lit(startTimeStr).cast("timestamp")
      val windowEnd = expr(s"timestamp('$startTimeStr') + INTERVAL $windowHours HOURS")

      val normalizedTimeStr = startTimeStr.replace("T", " ").stripSuffix("Z").trim
      val startMillis = java.sql.Timestamp.valueOf(normalizedTimeStr).getTime
      val windowEndMillis = startMillis + windowHours.toLong * 3600000L

      // Both one-hop edges and precomputed two-hop compound edges get folded
      // into the same (src, start, end, dst) shape, so the traversal below
      // doesn't need to know which kind of edge it's looking at.
      def loadAdjacency(): Map[Long, Array[(Long, Long, Long)]] = {
        val oneHop = edgesTable
          .filter($"sent_at" >= startTime && $"sent_at" <= windowEnd)
          .select(
            $"src".alias("src"),
            $"sent_at".alias("start"),
            $"sent_at".alias("end"),
            $"dst".alias("dst")
          )

        // Both hops of the compound edge must fall within the query window --
        // t1 is the departure, t2 is the arrival at n3.
        val twoHop = twoHopTable
          .filter($"t1" >= startTime && $"t2" <= windowEnd)
          .select(
            $"n1".alias("src"),
            $"t1".alias("start"),
            $"t2".alias("end"),
            $"n3".alias("dst")
          )

        val rows = oneHop.unionByName(twoHop)
          .as[TemporalEdge]
          .collect()

        rows
          .groupBy(_.src)
          .map { case (src, arr) =>
            src -> arr.map(e => (e.start.getTime, e.end.getTime, e.dst))
          }
      }

      // adj: node -> Array[(start, end, dst)]. An edge/compound-edge can be
      // taken from `node` at arrival time `t` iff start >= t (it's available
      // no earlier than our current arrival), and taking it advances our
      // arrival at `dst` to `end`.
      def Reachability(adj: Map[Long, Array[(Long, Long, Long)]]): Map[Long, Long] = {
        val arrival = mutable.Map[Long, Long](startNodeId -> startMillis)
        val settled = mutable.Set.empty[Long]
        val pq = mutable.PriorityQueue.empty[(Long, Long)](Ordering.by((p: (Long, Long)) => -p._2))
        pq.enqueue((startNodeId, startMillis))

        while (pq.nonEmpty) {
          val (node, t) = pq.dequeue()
          if (!settled.contains(node)) {
            settled += node
            adj.getOrElse(node, Array.empty).foreach { case (start, end, dst) =>
              if (start >= t && (!arrival.contains(dst) || end < arrival(dst))) {
                arrival(dst) = end
                pq.enqueue((dst, end))
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