name              := "SparkMultiGraphMatching"
version           := "1.0.0"
scalaVersion      := "2.12.15"
Global / logLevel := Level.Warn
run / outputStrategy := Some(StdoutOutput)

val sparkVersion   = "3.3.0"
val icebergVersion = "1.3.1"

resolvers += "Spark Packages Repo" at "https://repos.spark-packages.org/"

libraryDependencies ++= Seq(
  "org.apache.spark"       %% "spark-core"      % sparkVersion,
  "org.apache.spark"       %% "spark-sql"       % sparkVersion,
  "org.apache.iceberg"     %% "iceberg-spark-runtime-3.3" % icebergVersion,
  "org.apache.iceberg"     % "iceberg-core"     % icebergVersion,
  "org.apache.iceberg"     % "iceberg-aws"      % icebergVersion,
  "org.apache.spark"       %% "spark-graphx"    % sparkVersion,
  "software.amazon.awssdk" % "s3"               % "2.17.52",
  "org.apache.hadoop"      % "hadoop-aws"       % "3.3.2"
    exclude("org.slf4j", "slf4j-log4j12"),
  "org.apache.hadoop"      % "hadoop-common"    % "3.3.2"
    exclude("org.slf4j", "slf4j-log4j12"),
  "com.amazonaws"          % "aws-java-sdk-bundle" % "1.12.262",
  "org.scala-lang.modules" %% "scala-collection-compat" % "2.11.0",
  "graphframes" % "graphframes" % "0.8.2-spark3.2-s_2.12"
)

// Spark 3.3.0's bundled jackson-module-scala requires Jackson 2.13.x;
// hadoop-aws/aws-sdk transitively pull in 2.14.x, which breaks Spark's
// internal ObjectMapper init. Force the version Spark expects.
dependencyOverrides ++= Seq(
  "com.fasterxml.jackson.core" % "jackson-databind"    % "2.13.4.1",
  "com.fasterxml.jackson.core" % "jackson-core"        % "2.13.4",
  "com.fasterxml.jackson.core" % "jackson-annotations" % "2.13.4"
)

// Run in a forked JVM (required for javaOptions below to take effect),
// with the same --add-opens flags spark-submit applies automatically
// for JDK 17+ compatibility (Spark reflectively touches sun.nio.ch.DirectBuffer
// and other JDK-internal classes that the module system blocks by default).
fork := true

javaOptions ++= Seq(
  "-Xmx6g",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.text=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
)