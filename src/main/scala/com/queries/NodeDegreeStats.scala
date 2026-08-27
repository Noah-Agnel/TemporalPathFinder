package com.sparkmultigraph

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.sparkconfiguration.SparkHandler._

object NodeDegreeStats {

  def main(args: Array[String]): Unit = {
    val dbName = if (args.length > 0) args(0) else "graph_db"

    val spark = spConfig("Node Degree Stats")
    import spark.implicits._

    try {
      val edges = spark.table(s"$dbName.edges")

      def buildDegreeQuery() = {
        val outDegree = edges.groupBy($"src".alias("node_id")).count().withColumnRenamed("count", "out_degree")
        val inDegree  = edges.groupBy($"dst".alias("node_id")).count().withColumnRenamed("count", "in_degree")

        outDegree
          .join(inDegree, Seq("node_id"), "full_outer")
          .na.fill(0, Seq("out_degree", "in_degree"))
          .withColumn("total_degree", $"out_degree" + $"in_degree")
      }

      val numRuns = 5
      val timings = scala.collection.mutable.ArrayBuffer[Double]()

      for (i <- 1 to numRuns) {
        val start = System.nanoTime()
        val count = buildDegreeQuery().count()
        val elapsed = (System.nanoTime() - start) / 1e9
        timings += elapsed
        println(f"Run $i: $elapsed%.3f seconds ($count nodes)")
      }

      println(f"\nCold (run 1):  ${timings.head}%.3f s")
      println(f"Warm (avg runs 2-$numRuns): ${timings.tail.sum / timings.tail.size}%.3f s")

      // Final aggregate: average and maximum total degree across all nodes
      println("\nDegree statistics:")
      val stats = buildDegreeQuery().agg(
        avg($"total_degree").alias("avg_degree"),
        max($"total_degree").alias("max_degree")
      )

      stats.show()

    } catch {
      case e: Exception =>
        println(s"Error running query: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }
}