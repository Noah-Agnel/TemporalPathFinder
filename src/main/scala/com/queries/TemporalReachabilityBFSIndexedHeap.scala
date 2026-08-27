package com.queries

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import com.sparkconfiguration.SparkHandler._

import scala.collection.mutable

object TemporalReachabilityBFSIndexedHeap {

  // A binary min-heap keyed by node id, with decrease-key support via a
  // position index. Unlike scala.collection.mutable.PriorityQueue, relaxing a
  // node that's already in the heap updates its existing slot in place
  // instead of pushing a duplicate entry that has to be popped and discarded
  // later.
  final class IndexedMinHeap(initialCapacity: Int = 1024) {
    private var nodes: Array[Long] = new Array[Long](initialCapacity)
    private var prios: Array[Long] = new Array[Long](initialCapacity)
    private val posOf: mutable.HashMap[Long, Int] = mutable.HashMap.empty
    private var size: Int = 0

    def nonEmpty: Boolean = size > 0
    def contains(node: Long): Boolean = posOf.contains(node)
    def priorityOf(node: Long): Long = prios(posOf(node))

    /** Inserts a node with the given priority, or, if it's already present
      * with a *worse* (larger) priority, decreases it in place. If the node
      * is already present with an equal-or-better priority, this is a no-op.
      * Returns which case occurred, purely for instrumentation. */
    def insertOrDecrease(node: Long, prio: Long): String = {
      posOf.get(node) match {
        case Some(idx) =>
          if (prio < prios(idx)) {
            prios(idx) = prio
            siftUp(idx)
            "decrease"
          } else {
            "noop"
          }
        case None =>
          ensureCapacity(size + 1)
          val idx = size
          nodes(idx) = node
          prios(idx) = prio
          posOf(node) = idx
          size += 1
          siftUp(idx)
          "insert"
      }
    }

    /** Removes and returns the (node, priority) with the smallest priority. */
    def extractMin(): (Long, Long) = {
      val topNode = nodes(0)
      val topPrio = prios(0)
      size -= 1
      if (size > 0) {
        nodes(0) = nodes(size)
        prios(0) = prios(size)
        posOf(nodes(0)) = 0
        siftDown(0)
      }
      posOf.remove(topNode)
      (topNode, topPrio)
    }

    private def ensureCapacity(needed: Int): Unit = {
      if (needed > nodes.length) {
        val newCap = math.max(needed, nodes.length * 2)
        nodes = java.util.Arrays.copyOf(nodes, newCap)
        prios = java.util.Arrays.copyOf(prios, newCap)
      }
    }

    private def swap(i: Int, j: Int): Unit = {
      val tn = nodes(i); nodes(i) = nodes(j); nodes(j) = tn
      val tp = prios(i); prios(i) = prios(j); prios(j) = tp
      posOf(nodes(i)) = i
      posOf(nodes(j)) = j
    }

    private def siftUp(startIdx: Int): Unit = {
      var idx = startIdx
      while (idx > 0) {
        val parent = (idx - 1) / 2
        if (prios(parent) > prios(idx)) {
          swap(parent, idx)
          idx = parent
        } else {
          idx = 0 // break
        }
      }
    }

    private def siftDown(startIdx: Int): Unit = {
      var idx = startIdx
      var continue = true
      while (continue) {
        val left = 2 * idx + 1
        val right = 2 * idx + 2
        var smallest = idx
        if (left < size && prios(left) < prios(smallest)) smallest = left
        if (right < size && prios(right) < prios(smallest)) smallest = right
        if (smallest == idx) {
          continue = false
        } else {
          swap(idx, smallest)
          idx = smallest
        }
      }
    }
  }

  // Accumulates nanosecond totals + counts for each phase of the while loop.
  class StepTimings {
    var extractMinNanos: Long = 0L
    var adjLookupNanos: Long = 0L
    var relaxNanos: Long = 0L
    var iterations: Int = 0
    var heapInsertCalls: Int = 0
    var heapDecreaseCalls: Int = 0
    var heapNoopCalls: Int = 0

    def add(other: StepTimings): Unit = {
      extractMinNanos += other.extractMinNanos
      adjLookupNanos += other.adjLookupNanos
      relaxNanos += other.relaxNanos
      iterations += other.iterations
      heapInsertCalls += other.heapInsertCalls
      heapDecreaseCalls += other.heapDecreaseCalls
      heapNoopCalls += other.heapNoopCalls
    }
  }

  def main(args: Array[String]): Unit = {
    val dbName = if (args.length > 0) args(0) else "graph_db"
    val startEmail = if (args.length > 1) args(1) else "bob.ambrocik@enron.com"
    val startTimeStr = if (args.length > 2) args(2) else "2001-10-05 00:00:00"
    val windowHours = if (args.length > 3) args(3).toInt else 100

    val spark = spConfig("Temporal Reachability BFS Indexed Heap")
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
      val windowEndMillis = startMillis + windowHours.toLong * 3600000L

      def loadAdjacency(): Map[Long, Array[(Long, Long)]] = {
        val rows = edgesTable
          .filter($"sent_at" >= startTime && $"sent_at" <= windowEnd)
          .select($"src".as[Long], $"dst".as[Long], $"sent_at".as[java.sql.Timestamp])
          .collect()

        rows
          .groupBy(_._1)
          .map { case (src, arr) =>
            src -> arr.map { case (_, dst, t) => (dst, t.getTime) }
          }
      }

      // Same algorithm as the PQ version: pop the node with the earliest
      // known arrival time, finalize it, relax its outgoing edges. The only
      // change is that relaxing an already-queued node now updates its
      // existing heap slot (decrease-key) instead of pushing a duplicate.
      def Reachability(adj: Map[Long, Array[(Long, Long)]], timings: StepTimings): Map[Long, Long] = {
        val finalized = mutable.Map[Long, Long]() // committed arrival times, once popped
        val heap = new IndexedMinHeap()
        heap.insertOrDecrease(startNodeId, startMillis)

        while (heap.nonEmpty) {
          timings.iterations += 1

          val t0 = System.nanoTime()
          val (node, t) = heap.extractMin()
          val t1 = System.nanoTime()
          timings.extractMinNanos += (t1 - t0)

          finalized(node) = t

          val neighbors = adj.getOrElse(node, Array.empty)
          val t2 = System.nanoTime()
          timings.adjLookupNanos += (t2 - t1)

          neighbors.foreach { case (dst, edgeTime) =>
            if (edgeTime >= t && !finalized.contains(dst)) {
              val outcome = heap.insertOrDecrease(dst, edgeTime)
              outcome match {
                case "insert"   => timings.heapInsertCalls += 1
                case "decrease" => timings.heapDecreaseCalls += 1
                case _          => timings.heapNoopCalls += 1
              }
            }
          }
          val t3 = System.nanoTime()
          timings.relaxNanos += (t3 - t2)
        }
        finalized.toMap
      }

      def buildQuery(runTimings: StepTimings): Map[Long, Long] = {
        val adj = loadAdjacency()
        val start = System.nanoTime()
        val result = Reachability(adj, runTimings)
        val elapsed = (System.nanoTime() - start) / 1e9
        println(f"Run: ${elapsed}%.3f seconds")
        result
      }

      val numRuns = 5
      val timings = mutable.ArrayBuffer[Double]()
      val allStepTimings = mutable.ArrayBuffer[StepTimings]()
      var lastResult: Map[Long, Long] = Map.empty

      def printStepAverages(t: StepTimings, label: String): Unit = {
        val iters = math.max(t.iterations, 1)
        println(s"  -- $label (${t.iterations} loop iterations) --")
        println(f"     avg extractMin:    ${t.extractMinNanos.toDouble / iters}%.1f ns/iter  (total ${t.extractMinNanos / 1e6}%.3f ms)")
        println(f"     avg adj lookup:    ${t.adjLookupNanos.toDouble / iters}%.1f ns/iter  (total ${t.adjLookupNanos / 1e6}%.3f ms)")
        println(f"     avg relax/foreach: ${t.relaxNanos.toDouble / iters}%.1f ns/iter  (total ${t.relaxNanos / 1e6}%.3f ms)")
        println(f"     heap calls -> insert: ${t.heapInsertCalls}, decrease: ${t.heapDecreaseCalls}, noop: ${t.heapNoopCalls}")
      }

      for (i <- 1 to numRuns) {
        val runStepTimings = new StepTimings()
        val start = System.nanoTime()
        lastResult = buildQuery(runStepTimings)
        val elapsed = (System.nanoTime() - start) / 1e9
        timings += elapsed
        allStepTimings += runStepTimings
        println(f"Run $i: $elapsed%.3f seconds (${lastResult.size} reachable nodes)")
        printStepAverages(runStepTimings, s"Run $i step breakdown")
      }

      println(f"\nCold (run 1):  ${timings.head}%.3f s")
      println(f"Warm (avg runs 2-$numRuns): ${timings.tail.sum / timings.tail.size}%.3f s")

      val overall = new StepTimings()
      allStepTimings.foreach(overall.add)
      printStepAverages(overall, "Overall (all runs combined)")

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
