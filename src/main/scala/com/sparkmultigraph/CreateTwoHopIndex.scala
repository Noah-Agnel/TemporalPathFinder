package com.sparkmultigraph

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.sparkconfiguration.SparkHandler._

object CreateTwoHopIndex {
  def main(args: Array[String]): Unit = {
    val dbName = if (args.length > 0) args(0) else "graph_db"
    val spark = spConfig("Two-Hop Temporal Index Builder")
    import spark.implicits._

    try {
      spark.sql(s"CREATE DATABASE IF NOT EXISTS $dbName")
      spark.sql(s"USE $dbName")
      val edges = spark.table(s"$dbName.edges")

      spark.sql(
        s"""
          CREATE TABLE IF NOT EXISTS $dbName.two_hop_index (
            n1 BIGINT,
            t1 TIMESTAMP,
            n2 BIGINT,
            t2 TIMESTAMP,
            n3 BIGINT
          ) USING ICEBERG
          PARTITIONED BY (bucket(16, n1))
        """)

      val e1 = edges.select($"src".alias("n1"), $"sent_at".alias("t1"), $"dst".alias("n2"))
      val e2 = edges.select($"src".alias("n2b"), $"sent_at".alias("t2"), $"dst".alias("n3"))

      val twoHopAll = e1.join(e2, e1("n2") === e2("n2b"))
        .filter($"t1" < $"t2")
        .select($"n1", $"t1", $"n2", $"t2", $"n3")

      // For a fixed start edge (n1, t1, n2) and destination n3, keep only the
      // earliest arrival t2 -- drop any later-arriving duplicate path.
      val twoHop = twoHopAll
        .groupBy($"n1", $"t1", $"n2", $"n3")
        .agg(min($"t2").alias("t2"))
        .select($"n1", $"t1", $"n2", $"t2", $"n3")

      twoHop.writeTo(s"$dbName.two_hop_index").append()

      val count = spark.table(s"$dbName.two_hop_index").count()
      println(s"Two-hop index built: $count rows")
      println("Sample:")
      spark.table(s"$dbName.two_hop_index").show(10, truncate = false)

    } finally {
      spark.stop()
    }
  }
}
