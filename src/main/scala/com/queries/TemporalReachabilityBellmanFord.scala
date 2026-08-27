package com.queries

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import com.sparkconfiguration.SparkHandler._

import scala.collection.mutable

object TemporalReachabilityBellmanFord {

  case class RoundStats(roundNum: Int, worklistSize: Long, improvedCount: Long, elapsedSeconds: Double)

  def main(args: Array[String]): Unit = {
    val dbName = if (args.length > 0) args(0) else "graph_db"
    val startEmail = if (args.length > 1) args(1) else "bob.ambrocik@enron.com"
    val startTimeStr = if (args.length > 2) args(2) else "2001-10-05 00:00:00"
    val windowHours = if (args.length > 3) args(3).toInt else 100

    val spark = spConfig("Temporal Reachability Corrected Worklist")
    spark.sparkContext.setCheckpointDir("/tmp/spark-checkpoints")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.shuffle.partitions", "8")
    import spark.implicits._

    try {
      val person = spark.table(s"$dbName.person")
      val edges  = spark.table(s"$dbName.edges")

      val startNodeId = person.filter($"email" === startEmail).select($"node_id").as[Long].collect().head
      val startTime  = lit(startTimeStr).cast("timestamp")
      val windowEnd  = expr(s"timestamp('$startTimeStr') + INTERVAL $windowHours HOURS")

      def buildQuery(): (DataFrame, Seq[RoundStats]) = {
        // Cache eligibleEdges: it's re-scanned as the join target every round, and it
        // doesn't change across rounds, so avoid re-filtering the base table each time.
        var prevTime = System.nanoTime()
        val eligibleEdges = edges.filter($"sent_at" >= startTime && $"sent_at" <= windowEnd).persist()

        var newTime = System.nanoTime()
        var elapsed = newTime - prevTime
        prevTime = newTime
        println(f"eligibleEdges built: ${elapsed / 1e9}%.3f s")

        var bestArrival = Seq((startNodeId, startTimeStr)).toDF("node_id", "arrival_time")
          .withColumn("arrival_time", $"arrival_time".cast("timestamp"))

        newTime = System.nanoTime()
        elapsed = newTime - prevTime
        prevTime = newTime
        println(f"bestArrival built: ${elapsed / 1e9}%.3f s")

        // Worklist starts as just the start node -- the only node whose state is "new"
        // at round 1. Each subsequent round's worklist is exactly last round's improved set.
        var worklist = bestArrival

        var continueLoop = true
        var iteration = 0
        val roundStats = mutable.ArrayBuffer[RoundStats]()
        val maxIterations = 100

        newTime = System.nanoTime()
        elapsed = newTime - prevTime
        prevTime = newTime
        println(f"Start of loop: ${elapsed / 1e9}%.3f s")

        while (continueLoop) {
          println("===========================================================================")
          println(s"============================== Start Round $iteration ==============================")
          println("===========================================================================")
          iteration += 1
          val roundStart = System.nanoTime()
          val worklistSize = worklist.count()

          newTime = System.nanoTime()
          elapsed = newTime - prevTime
          prevTime = newTime
          println(f"worklistSize counted: ${elapsed / 1e9}%.3f s")

          // Only join FROM nodes in the worklist -- not the full bestArrival table.
          // Nodes outside the worklist didn't change last round, so their outgoing
          // edges were already fully considered against their (unchanged) arrival time.
          val candidates = eligibleEdges.as("e")
            .join(worklist.as("w"), $"e.src" === $"w.node_id")
            .filter($"e.sent_at" >= $"w.arrival_time")
            .groupBy($"e.dst".alias("node_id"))
            .agg(min($"e.sent_at").alias("candidate_time"))

          newTime = System.nanoTime()
          elapsed = newTime - prevTime
          prevTime = newTime
          println(f"candidates built: ${elapsed / 1e9}%.3f s")

          // Still compare against the FULL bestArrival table -- a candidate is only a
          // real improvement if it beats the best arrival known from ANY path so far,
          // not just paths through the worklist.
          val improved = candidates.as("c")
            .join(bestArrival.as("b"), $"c.node_id" === $"b.node_id", "left_outer")
            .filter($"b.arrival_time".isNull || $"c.candidate_time" < $"b.arrival_time")
            .select($"c.node_id".alias("node_id"), $"c.candidate_time".alias("arrival_time"))
            .checkpoint()
        
          newTime = System.nanoTime()
          elapsed = newTime - prevTime
          prevTime = newTime
          println(f"improved built: ${elapsed / 1e9}%.3f s")

          val improvedCount = improved.count()
          val loopElapsedSeconds = (System.nanoTime() - roundStart).toDouble / 1e9
          roundStats += RoundStats(iteration, worklistSize, improvedCount, loopElapsedSeconds)

          newTime = System.nanoTime()
          elapsed = newTime - prevTime
          prevTime = newTime
          println(f"improvedCount built and roundstats modified: ${elapsed / 1e9}%.3f s")

          if (improvedCount == 0 || iteration > maxIterations) {
            continueLoop = false
          } else {
            bestArrival = bestArrival.as("old")
              .join(improved.select($"node_id".alias("imp_id")), $"old.node_id" === $"imp_id", "left_anti")
              .select($"old.node_id", $"old.arrival_time")
              .union(improved)
              .checkpoint()

            // Next round only propagates from what just changed.
            worklist = improved
          }
          newTime = System.nanoTime()
          elapsed = newTime - prevTime
          prevTime = newTime
          println(f"BestArrival modified: ${elapsed / 1e9}%.3f s")

          println("===========================================================================")
          println(s"============================== End Round ${iteration - 1} ==============================")
          println("===========================================================================")
        }

        eligibleEdges.unpersist()
        (bestArrival, roundStats.toSeq)
      }

      val numRuns = 1
      val timings = mutable.ArrayBuffer[Double]()
      val roundCounts = mutable.ArrayBuffer[Int]()
      var lastStats: Seq[RoundStats] = Seq.empty
      var lastCount: Long = 0L

      for (i <- 1 to numRuns) {
        val start = System.nanoTime()
        val (result, stats) = buildQuery()
        val count = result.count()
        val elapsed = (System.nanoTime() - start) / 1e9
        timings += elapsed
        roundCounts += stats.size
        lastStats = stats
        lastCount = count
        println(f"Run $i: $elapsed%.3f seconds ($count reachable nodes, ${stats.size} rounds)")
      }

      println(f"\nCold (run 1):  ${timings.head}%.3f s")
      println(f"Warm (avg runs 2-$numRuns): ${if (timings.tail.isEmpty) 0.0 else timings.tail.sum / timings.tail.size}%.3f s")
      println(f"Avg rounds across runs: ${roundCounts.sum.toDouble / roundCounts.size}%.1f")

      println("\nPer-round breakdown (final run):")
      println(f"${"Round"}%6s ${"Worklist"}%10s ${"Improved"}%10s ${"Seconds"}%10s")
      lastStats.foreach { s =>
        println(f"${s.roundNum}%6d ${s.worklistSize}%10d ${s.improvedCount}%10d ${s.elapsedSeconds}%10.3f")
      }
      println(f"Total round time (final run): ${lastStats.map(_.elapsedSeconds).sum}%.3f s " +
        f"across ${lastStats.size} rounds, final count = $lastCount")

      println("\nReachable people:")
      val (finalResult, _) = buildQuery()
      finalResult
        .join(person, "node_id")
        .select($"email", $"arrival_time")
        .orderBy("arrival_time")
        .show(100, truncate = false)

    } catch {
      case e: Exception =>
        println(s"Error running query: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }
}