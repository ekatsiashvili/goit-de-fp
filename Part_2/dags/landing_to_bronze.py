import os
import logging
import requests
from datetime import datetime
from pyspark.sql import SparkSession

# Налаштування логування
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

def download_data(table_name, landing_dir):
    """Завантаження CSV з FTP-серверу"""
    url = f"https://ftp.goit.study/neoversity/{table_name}.csv"
    local_path = os.path.join(landing_dir, f"{table_name}.csv")

    logger.info(f"📥 Завантаження: {url}")
    response = requests.get(url)
    if response.status_code == 200:
        with open(local_path, "wb") as f:
            f.write(response.content)
        logger.info(f"✅ CSV збережено: {local_path}")
    else:
        logger.error(f"❌ Не вдалося завантажити {url} (Status code: {response.status_code})")
        exit(1)

def ensure_directories_exist(*dirs):
    """Створення директорій, якщо вони не існують"""
    for dir_path in dirs:
        os.makedirs(dir_path, exist_ok=True)
        logger.info(f"📁 Директорія гарантовано існує: {dir_path}")

def process_table(spark, table_name, landing_dir, bronze_dir):
    """Завантаження, читання та збереження даних у bronze/{table}/{timestamp}"""
    download_data(table_name, landing_dir)

    csv_path = os.path.join(landing_dir, f"{table_name}.csv")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    bronze_path = os.path.join(bronze_dir, table_name, timestamp)

    logger.info(f"📊 Обробка файлу: {csv_path}")
    df = spark.read.option("header", "true").csv(csv_path)

    df.printSchema()
    df.show(5, truncate=False)

    df.write.parquet(bronze_path)
    logger.info(f"💾 Дані збережено у Parquet: {bronze_path}")

def main():
    spark = SparkSession.builder.appName("LandingToBronze").getOrCreate()

    landing_dir = "/tmp/spark_data/landing"
    bronze_dir = "/tmp/spark_data/bronze"
    ensure_directories_exist(landing_dir, bronze_dir)

    tables = ["athlete_bio", "athlete_event_results"]

    for table in tables:
        process_table(spark, table, landing_dir, bronze_dir)

    spark.stop()

if __name__ == "__main__":
    main()
