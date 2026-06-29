package sink

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

object GoldToPostgres {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val spark = SparkSession.builder()
      .appName("gold-to-postgres")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // 1. Recalculer gold depuis silver (batch)
    val silver = spark.read.format("delta").load("C:/tmp/delta/silver_trades")

    val gold = silver
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

    // 2. Écrire dans Postgres via JDBC
    gold.write
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5432/crypto")
      .option("dbtable", "gold_metrics")
      .option("user", "crypto")
      .option("password", "crypto")
      .option("driver", "org.postgresql.Driver")
      .mode(SaveMode.Overwrite)
      .save()

    println("✅ Gold écrit dans Postgres !")

    spark.stop()
  }
}