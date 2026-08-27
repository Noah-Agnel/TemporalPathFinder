# temporalPathFinder

Temporal path-finding and reachability engine: given a source node and a
start time, finds the earliest-arrival path (or full reachability set) to
other nodes in a graph whose edges are timestamped, respecting the constraint
that edges along any path must be traversed in non-decreasing time order.
Built on Scala, Apache Spark, and Apache Iceberg; demonstrated against the
Enron email corpus, where each email is a temporal edge between two people.
