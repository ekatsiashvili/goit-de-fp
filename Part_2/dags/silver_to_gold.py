import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, current_timestamp
from datetime import datetime

# Налаштування логування
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

def get_latest_subdir(base_path):
    try:
        subdirs = [
            os.path.join(base_path, d)
            for d in os.listdir(base_path)
            if os.path.isdir(os.path.join(base_path, d))
        ]
        return max(subdirs, key=os.path.getmtime) if subdirs else None
    except FileNotFoundError:
        return None

def process_to_gold(spark, silver_dir, gold_dir):
    today = datetime.today().strftime("%Y%m%d")
    silver_events_path = os.path.join(silver_dir, "athlete_event_results", today)

    logger.info(f"📁 Очікуваний шлях до silver athlete_event_results: {silver_events_path}")

    if not os.path.exists(silver_events_path):
        logger.error("❌ Шлях до athlete_event_results не існує.")
        return

    df_events = spark.read.parquet(silver_events_path)

    logger.info("📊 Агрувація кількості медалей за sport, country_noc, medal")
    df_gold = df_events.groupBy("sport", "country_noc", "medal") \
        .agg(count("*").alias("medal_count")) \
        .withColumn("timestamp", current_timestamp())

    logger.info("📑 Схема фінального gold-фрейму:")
    df_gold.printSchema()
    df_gold.show(10, truncate=False)

    gold_results_path = os.path.join(gold_dir, f"medal_summary_{today}")
    logger.info(f"💾 Збереження результату у gold: {gold_results_path}")
    df_gold.write.mode("overwrite").parquet(gold_results_path)

    logger.info("✅ Збережено успішно!")

def main():
    silver_dir = "/tmp/spark_data/silver"
    gold_dir = "/tmp/spark_data/gold"

    os.makedirs(gold_dir, exist_ok=True)

    spark = SparkSession.builder \
        .appName("SilverToGold") \
        .getOrCreate()

    process_to_gold(spark, silver_dir, gold_dir)

    spark.stop()

if __name__ == "__main__":
    main()
