name := "realtime-crypto-pipeline"

scalaVersion := "2.13.14"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.1"

libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.1"

libraryDependencies += "io.delta" %% "delta-spark" % "3.2.0"

libraryDependencies += "org.postgresql" % "postgresql" % "42.7.3"