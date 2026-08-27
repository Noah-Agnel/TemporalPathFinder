# temporalPathFinder

Temporal path-finding and reachability engine: given a source node and a
start time, finds the earliest-arrival path (or full reachability set) to
other nodes in a graph whose edges are timestamped, respecting the constraint
that edges along any path must be traversed in non-decreasing time order.
Built on Scala, Apache Spark, and Apache Iceberg; demonstrated against the
Enron email corpus, where each email is a temporal edge between two people.

## What's in `queries`

Rather than one canonical implementation, this package is a set of
alternative solutions to the same reachability problem, each isolating one
performance idea:

- **Baselines** — `TemporalReachabilitySQLRecursive` (pure Spark SQL
  `WITH RECURSIVE`) and `GraphFramesTemporalReachability` (GraphFrames'
  Pregel API, bulk-synchronous message passing).
- **In-memory Dijkstra/BFS** — `TemporalReachabilityBFS` (Scala's built-in
  `PriorityQueue`) and `TemporalReachabilityBFSIndexedHeap` (a hand-written
  indexed binary min-heap with real decrease-key support).
- **Two-hop index** — `CreateTwoHopIndex` precomputes compound two-hop
  connections; `TemporalReachabilityBFSTwoHop` traverses a unified 1-hop +
  2-hop adjacency built from it, and `TwoHopPathSearch` uses the index
  directly for point-to-point queries.
- **Chronological / distributed** — `TemporalReachabilityStream` (single
  sorted linear pass, no heap) and `TemporalReachabilityBellmanFord` (the
  only fully distributed variant — a Spark-native worklist relaxation).
- `NodeDegreeStats` — in/out-degree aggregation over the same dataset.

## Stack

Scala 2.12.15, Apache Spark 3.3.0, Apache Iceberg 1.3.1, GraphFrames
0.8.2 (used by one baseline implementation). `spark-graphx` is listed as a
build dependency but is not currently imported anywhere in the code. Unlike
`MultiQuery`, this repo has no bundled Docker stack — it assumes an
equivalent local Spark/Iceberg (and, for MinIO-backed reads, S3A) setup is
already available.

## Getting started

Requires JDK 17 (Spark's reflective access needs the `--add-opens` flags
already baked into `build.sbt`).

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
export AWS_ACCESS_KEY_ID="minioadmin"
export AWS_SECRET_ACCESS_KEY="minioadmin"

sbt "runMain com.sparkmultigraph.queries.TemporalReachabilityBFS \
  graph_db bob.ambrocik@enron.com '2001-10-05 00:00:00' 100"
```

Every query class follows the same `<dbName> <startEmail> <startTime> <windowHours>`
argument pattern and prints cold/warm timing stats.

## Known gaps

- No benchmark numbers have been captured yet (every query prints cold/warm
  timings to stdout, but none has been saved to a file).
- The driver-collecting implementations (`TemporalReachabilityBFS` and its
  heap/streaming variants) `.collect()` the entire in-window edge set to the
  driver — fine at Enron's scale, but won't scale past driver memory.

## Related repos

- [`MultiQuery`](../MultiQuery) — the subgraph-matching half of this
  research effort, over the ICIJ Panama Papers dataset.
- [`PostgreSQLMultiGraphMatch`](../PostgreSQLMultiGraphMatch) — a
  Python/PostgreSQL port of `MultiQuery`'s matching pipeline.
