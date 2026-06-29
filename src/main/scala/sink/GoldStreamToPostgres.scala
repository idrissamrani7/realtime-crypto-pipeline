package sink

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object GoldStreamToPostgres {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val spark = SparkSession.builder()
      .appName("gold-stream-to-postgres")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // 1. Lire SILVER en streaming
    val silver = spark.readStream
      .format("delta")
      .load("C:/tmp/delta/silver_trades")

    // 2. Agréger par fenêtre de 10s (logique gold habituelle)
    val gold = silver
      .withWatermark("event_time", "30 seconds")
      .groupBy(window(col("event_time"), "10 seconds"), col("symbol"))
      .agg(
        avg("price").as("prix_moyen"),
        min("price").as("prix_min"),
        max("price").as("prix_max"),
        sum("quantity").as("volume"),
        count("*").as("nb_trades")
      )
      .select(
        col("symbol"),
        col("window.start").as("window_start"),
        col("window.end").as("window_end"),
        col("prix_moyen"),
        col("prix_min"),
        col("prix_max"),
        col("volume"),
        col("nb_trades")
      )

    // 3. Écrire chaque micro-batch dans Postgres via foreachBatch
    gold.writeStream
      .outputMode("update")
      .option("checkpointLocation", "C:/tmp/checkpoints/gold_postgres")
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        batchDF.write
          .format("jdbc")
          .option("url", "jdbc:postgresql://localhost:5432/crypto")
          .option("dbtable", "gold_metrics")
          .option("user", "crypto")
          .option("password", "crypto")
          .option("driver", "org.postgresql.Driver")
          .mode(SaveMode.Append)
          .save()
        println(s"✅ Batch $batchId écrit dans Postgres")
      }
      .start()
      .awaitTermination()
  }
}