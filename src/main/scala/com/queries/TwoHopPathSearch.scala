package com.queries

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import com.sparkconfiguration.SparkHandler._

object TwoHopPathSearch {

  def findPath(
      spark: SparkSession,
      dbName: String,
      startX: Long,
      targetY: Long,
      marginInSeconds: Long = 0,
      maxDepth: Int = 10
  ): DataFrame = {
    import spark.implicits._

    val twoHop = spark.table(s"$dbName.two_hop_index")

    // Step 1: Base case (new0) -> Select two-hop records where c1 = X
    var currentNew: DataFrame = twoHop
      .filter($"n1" === startX)
      .cache()

    // Check if target Y is reached directly within 2 hops (c2 = Y or c3 = Y)
    var poss: DataFrame = currentNew.filter($"n2" === targetY || $"n3" === targetY)

    // Step 2: Loop if not found in base case
    var flag = true
    var depth = 1

    while (flag && depth <= maxDepth) {
      if (!poss.isEmpty) {
        println(s"Target $targetY reached at depth/step $depth!")
        flag = false
      } else {
        println(s"Expanding search... Iteration $depth")

        // Join twohop with new_($count - 1) N
        // Join Condition: twohop.c1 == N.c3 AND N.t2 + margin < twohop.t1
        val expanded = twoHop.as("T")
          .join(
            currentNew.as("N"),
            col("T.n1") === col("N.n3") && 
            col("N.t2") + expr(s"INTERVAL $marginInSeconds SECONDS") < col("T.t1")
          )
          // Select twohop attributes for the next frontier
          .select(
            col("T.n1"),
            col("T.t1"),
            col("T.n2"),
            col("T.t2"),
            col("T.n3")
          )
          .distinct()
          .cache()

        // Update current frontier (new_$count)
        currentNew.unpersist() // Free previous iteration memory
        currentNew = expanded

        // Check reachability in new step
        poss = currentNew.filter($"n2" === targetY || $"n3" === targetY)

        if (poss.head(1).nonEmpty) {
          println(s"Target $targetY reached at iteration $depth!")
          flag = false
        } else {
          depth += 1
        }
      }
    }

    poss
  }

  def main(args: Array[String]): Unit = {
    val dbName = if (args.length > 0) args(0) else "graph_db"
    val spark = spConfig("Temporal Reachability Query")

    try {
      val startNode = 101L  // Source X
      val targetNode = 505L // Destination Y
      val margin = 0L       // Time delay margin in seconds

      val resultDF = findPath(spark, dbName, startNode, targetNode, margin)
      resultDF.explain(true)

      println("Matching paths (poss):")
      resultDF.show(10, truncate = false)

    } finally {
      spark.stop()
    }
  }
}