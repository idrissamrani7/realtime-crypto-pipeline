package pipeline

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Main {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val spark = SparkSession.builder()
      .appName("crypto-pipeline")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // 1. Lire le flux Kafka
    val kafka = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "crypto-trades")
      .option("startingOffsets", "latest")
      .load()

    // 2. Schéma du JSON Binance
    val schema = new StructType()
      .add("s", StringType)
      .add("p", StringType)
      .add("q", StringType)
      .add("T", LongType)

    // 3. Parser + typer
    val trades = kafka
      .selectExpr("CAST(value AS STRING) AS json")
      .select(from_json(col("json"), schema).as("data"))
      .select(
        col("data.s").as("symbol"),
        col("data.p").cast("double").as("price"),
        col("data.q").cast("double").as("quantity"),
        (col("data.T") / 1000).cast("timestamp").as("event_time")
      )

    // 4. BRONZE → écrire les trades bruts dans une table Delta
    trades.writeStream
      .format("delta")
      .outputMode("append")
      .option("checkpointLocation", "C:/tmp/checkpoints/bronze")
      .start("C:/tmp/delta/bronze_trades")
      .awaitTermination()
  }
}