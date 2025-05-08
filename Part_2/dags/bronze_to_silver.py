from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import re
import os
from datetime import datetime

def clean_text(text):
    if text is None:
        return ""
    return re.sub(r'[^a-zA-Z0-9\s]', '', text.lower())

def clean_dataframe(df, spark):
    for column, dtype in df.dtypes:
        if dtype == "string":
            clean_udf = spark.udf.register(f"clean_{column}", clean_text)
            df = df.withColumn(column, clean_udf(col(column)))
    return df

def process_table(spark, table_name):
    today = datetime.today().strftime("%Y%m%d")
    bronze_path = f"/tmp/spark_data/bronze/{table_name}/{today}_*"
    silver_path = f"/tmp/spark_data/silver/{table_name}/{today}"

    print(f"🔍 Читання з: {bronze_path}")
    df = spark.read.format("parquet").load(bronze_path)

    print("📑 Схема початкового датафрейму:")
    df.printSchema()

    print("🔎 Перші 5 рядків з Bronze:")
    df.show(5, truncate=False)

    print(f"🧼 Чищення датафрейму...")
    df_cleaned = clean_dataframe(df, spark)

    print("📑 Схема очищеного датафрейму:")
    df_cleaned.printSchema()

    print("🔎 Перші 10 рядків після очищення:")
    df_cleaned.show(10, truncate=False)

    print(f"💾 Збереження до Silver рівня: {silver_path}")
    df_cleaned.write.mode("overwrite").parquet(silver_path)
    print(f"✅ Збережено до Silver: {silver_path}")

    print("📁 Шлях збереження Silver:", silver_path)
    print("📂 Вміст директорії:")
    print(os.listdir(silver_path))

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Bronze to Silver ETL") \
        .getOrCreate()

    # Список таблиць, які потрібно обробити
    tables = ["athlete_event_results"]

    for table in tables:
        process_table(spark, table)

    spark.stop()
