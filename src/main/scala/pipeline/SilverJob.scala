package pipeline

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SilverJob {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val spark = SparkSession.builder()
      .appName("silver-job")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // 1. Lire la table BRONZE (en flux)
    val bronze = spark.readStream
      .format("delta")
      .load("C:/tmp/delta/bronze_trades")

    // 2. Nettoyer
    val silver = bronze
      .filter(col("price") > 0)                         // écarte les prix aberrants
      .filter(col("quantity") > 0)                      // écarte les quantités nulles
      .withWatermark("event_time", "1 minute")          // borne temporelle pour le dédoublonnage
      .dropDuplicates("symbol", "price", "quantity", "event_time")  // supprime les doublons

    // 3. Écrire la table SILVER
    silver.writeStream
      .format("delta")
      .outputMode("append")
      .option("checkpointLocation", "C:/tmp/checkpoints/silver")
      .start("C:/tmp/delta/silver_trades")
      .awaitTermination()
  }
}