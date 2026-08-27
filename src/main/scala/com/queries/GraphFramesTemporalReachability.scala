package com.queries

import org.apache.spark.sql.functions._
import com.sparkconfiguration.SparkHandler._
import org.graphframes.GraphFrame
import org.graphframes.lib.Pregel

object GraphFramesTemporalReachability {

  def main(args: Array[String]): Unit = {
    val dbName = if (args.length > 0) args(0) else "graph_db"
    val startEmail = if (args.length > 1) args(1) else "bob.ambrocik@enron.com"
    val startTimeStr = if (args.length > 2) args(2) else "2001-10-05 00:00:00"
    val windowHours = if (args.length > 3) args(3).toInt else 100

    val spark = spConfig("GraphFrames Temporal Reachability")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    
    // Set checkpoint directory required by GraphFrames Pregel
    spark.sparkContext.setCheckpointDir("/tmp/spark-checkpoints")

    import spark.implicits._

    try {
      // --- 1. LOAD & CACHE GRAPH ONCE ---
      println("\n==> Loading and caching graph data...")
      val person = spark.table(s"$dbName.person")
      val edgesTable = spark.table(s"$dbName.edges")

      val startNodeId = person.filter($"email" === startEmail).select($"node_id").as[Long].collect().head

      val filteredEdges = edgesTable
        .filter($"sent_at" >= to_timestamp(lit(startTimeStr)) && 
                $"sent_at" <= expr(s"TIMESTAMP '$startTimeStr' + INTERVAL $windowHours HOURS"))
        .select($"src", $"dst", $"sent_at")

      val vertices = filteredEdges.select($"src".as("id"))
        .union(filteredEdges.select($"dst".as("id")))
        .distinct()

      val initialVertices = vertices.withColumn(
        "initial_arrival_time",
        when($"id" === startNodeId, to_timestamp(lit(startTimeStr))).otherwise(lit(null))
      )

      // Cache graph data to isolate algorithm timing from initial table load/parsing
      initialVertices.cache()
      filteredEdges.cache()
      
      // Force cache materialization before benchmark
      val vCount = initialVertices.count()
      val eCount = filteredEdges.count()
      println(s"Graph loaded into memory: $vCount vertices, $eCount edges.")

      val gf = GraphFrame(initialVertices, filteredEdges)

      // --- 2. BENCHMARK PREGEL QUERY (5 RUNS) ---
      val numRuns = 5
      val runtimes = new scala.collection.mutable.ArrayBuffer[Long]()

      println(s"\n==> Executing GraphFrames Pregel Temporal Reachability ($numRuns runs)...")

      for (i <- 1 to numRuns) {
        val startTime = System.currentTimeMillis()

        // Construct Pregel Job
        val resultGF = gf.pregel
          .withVertexColumn(
            "arrival_time",
            col("initial_arrival_time"),
            when(Pregel.msg.isNotNull, 
              when(col("arrival_time").isNull, Pregel.msg)
                .otherwise(least(col("arrival_time"), Pregel.msg))
            ).otherwise(col("arrival_time"))
          )
          .sendMsgToDst(
            when(
              Pregel.src("arrival_time").isNotNull && Pregel.edge("sent_at") >= Pregel.src("arrival_time"),
              Pregel.edge("sent_at")
            )
          )
          .aggMsgs(min(Pregel.msg))
          .setMaxIter(50)
          .run()

        val finalReachability = resultGF
          .filter($"arrival_time".isNotNull)
          .select(
            $"id".as("node_id"),
            (unix_timestamp($"arrival_time") * 1000).cast("bigint").as("arrival_millis")
          )

        // Action forces execution and triggers computation end-to-end
        val totalReachable = finalReachability.count()

        val duration = System.currentTimeMillis() - startTime
        runtimes += duration

        println(f"Run $i%d: $duration%d ms | Reachable nodes: $totalReachable%d")
      }

      // --- 3. PRINT SUMMARY ---
      println("\n" + "=" * 40)
      println("BENCHMARK RESULTS (Pregel Query Runtime)")
      println("=" * 40)
      runtimes.zipWithIndex.foreach { case (time, idx) =>
        println(f"Run ${idx + 1}%d: $time%4d ms")
      }
      val avgTime = runtimes.sum.toDouble / numRuns
      println("-" * 40)
      println(f"Average Runtime: $avgTime%.2f ms")
      println("=" * 40)

    } finally {
      spark.stop()
    }
  }
}