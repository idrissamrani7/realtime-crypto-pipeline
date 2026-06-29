package pipeline

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object GoldJob {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val spark = SparkSession.builder()
      .appName("gold-job")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // 1. Lire SILVER en flux
    val silver = spark.readStream
      .format("delta")
      .load("C:/tmp/delta/silver_trades")

    // 2. Agréger par fenêtre de 10 secondes
    val gold = silver
      .withWatermark("event_time", "30 seconds")
      .groupBy(
        window(col("event_time"), "10 seconds"),
        col("symbol")
      )
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

    // 3. Écrire GOLD en Delta
    gold.writeStream
      .format("delta")
      .outputMode("append")
      .option("checkpointLocation", "C:/tmp/checkpoints/gold")
      .start("C:/tmp/delta/gold_metrics")
      .awaitTermination()
  }
}