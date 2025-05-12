import os
import json
from dotenv import load_dotenv
from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, to_json, col, struct, avg, current_timestamp, regexp_replace, count
)
from pyspark.sql.types import (
    StructType, StringType, IntegerType, FloatType
)

# Завантаження змінних середовища
load_dotenv()

# ======== Налаштування середовища для Spark =========
os.environ['PYSPARK_SUBMIT_ARGS'] = (
    '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,'
    'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'
)

# ==== Конфігурація MySQL  =====
mysql_config = {
    "url": f"jdbc:mysql://{os.getenv('MYSQL_HOST')}:{os.getenv('MYSQL_PORT')}/{os.getenv('MYSQL_DB')}",
    "bio_table": "athlete_bio",
    "results_table": "athlete_event_results",
    "agg_table": "olena_agg_res",  
    "user": os.getenv("MYSQL_USER"),
    "password": os.getenv("MYSQL_PASSWORD"),
    "driver": os.getenv("MYSQL_DRIVER")
}

# ==== Конфігурація Kafka з .env =====
kafka_config = {
    "bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "").split(","),
    "security_protocol": os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
    "event_topic": os.getenv("KAFKA_EVENT_TOPIC"),
    "output_topic": os.getenv("KAFKA_OUTPUT_TOPIC")
}

# ======== Створення Spark-сесії =========
spark = SparkSession.builder \
    .appName("MyUnifiedApp") \
    .config("spark.jars", "/opt/jars/mysql-connector-j-8.0.32.jar,/opt/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar,/opt/jars/kafka-clients-3.5.1.jar") \
    .master("local[*]") \
    .getOrCreate()

# ======== Читання результатів із MySQL =========
results_df = spark.read.format('jdbc').options(
    url=mysql_config["url"],
    driver=mysql_config["driver"],
    dbtable=mysql_config["results_table"],
    user=mysql_config["user"],
    password=mysql_config["password"]
).load()

# ======== Kafka-продюсер =========
producer = KafkaProducer(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: str(k).encode('utf-8'),
    request_timeout_ms=60000,  # збільшити час тайм-ауту
    retries=5  # кількість спроб підключення
)

for row in results_df.toJSON().collect():
    data = json.loads(row)
    key = str(data["athlete_id"])
    producer.send(kafka_config["event_topic"], key=key, value=data)

producer.flush()
producer.close()

# ======== Схема Kafka-повідомлення =========
results_schema = StructType() \
    .add("edition", StringType()) \
    .add("edition_id", IntegerType()) \
    .add("country_noc", StringType()) \
    .add("sport", StringType()) \
    .add("event", StringType()) \
    .add("result_id", StringType()) \
    .add("athlete", StringType()) \
    .add("athlete_id", IntegerType()) \
    .add("pos", StringType()) \
    .add("medal", StringType()) \
    .add("isTeamSport", StringType())

# ======== Стрім з Kafka =========
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", ",".join(kafka_config["bootstrap_servers"])) \
    .option("subscribe", kafka_config["event_topic"]) \
    .option("startingOffsets", "latest") \
    .option("kafka.security.protocol", kafka_config["security_protocol"]) \
    .load()

parsed_df = df_kafka.selectExpr("CAST(value AS STRING)") \
    .withColumn("json", from_json(col("value"), results_schema)) \
    .select("json.*")

# ======== Біо-дані =========
bio_df = spark.read.format('jdbc').options(
    url=mysql_config["url"],
    driver=mysql_config["driver"],
    dbtable=mysql_config["bio_table"],
    user=mysql_config["user"],
    password=mysql_config["password"]
).load()

expected_columns = ["athlete_id", "height", "weight"]
missing_columns = [col_name for col_name in expected_columns if col_name not in bio_df.columns]

if missing_columns:
    print(f"⚠️ Відсутні колонки: {missing_columns} — обробка bio_clean_df пропущена")
    bio_clean_df = bio_df  
else:
    bio_clean_df = bio_df \
        .withColumn("height", regexp_replace(col("height"), ",", ".")) \
        .withColumn("weight", regexp_replace(col("weight"), ",", ".")) \
        .withColumn("height", col("height").cast(FloatType())) \
        .withColumn("weight", col("weight").cast(FloatType())) \
        .withColumn("athlete_id", col("athlete_id").cast(IntegerType())) \
        .filter(col("height").isNotNull() & col("weight").isNotNull())

# ======== Додавання типу athlete_id в parsed_df та watermark =========
parsed_df = parsed_df \
    .withColumn("athlete_id", col("athlete_id").cast(IntegerType())) \
    .withColumn("event_time", current_timestamp()) \
    .withWatermark("event_time", "1 minute")

# ======== Об'єднання та агрегація =========
enriched_df = parsed_df.join(bio_clean_df, on="athlete_id", how="inner") \
    .select(
        parsed_df["sport"],
        parsed_df["medal"],
        bio_clean_df["sex"],
        bio_clean_df["country_noc"].alias("bio_country_noc"),  
        bio_clean_df["height"],
        bio_clean_df["weight"]
    )

df_aggregated = enriched_df.groupBy("sport", "medal", "sex", "bio_country_noc").agg(
    avg("height").alias("avg_height"),
    avg("weight").alias("avg_weight"),
    count("*").alias("athlete_count")
).withColumn("timestamp", current_timestamp())

# ======== Функція запису результатів =========
def process_batch(batch_df, batch_id):
    print(f"=== Processing batch {batch_id} ===")
    
    if batch_df.count() == 0:
        print("⚠️  Empty batch — nothing to process.")
        return

    # Вивід вмісту batch_df у консоль (для дебагу)
    batch_df.show(truncate=False)

    # Kafka
    batch_df.selectExpr("CAST(sport AS STRING) AS key", "to_json(struct(*)) AS value") \
        .write.format("kafka") \
        .option("kafka.bootstrap.servers", ",".join(kafka_config["bootstrap_servers"])) \
        .option("topic", kafka_config["output_topic"]) \
        .save()

    # MySQL
    batch_df.write.format("jdbc").options(
        url=mysql_config["url"],
        driver=mysql_config["driver"],
        dbtable=mysql_config["agg_table"],
        user=mysql_config["user"],
        password=mysql_config["password"]
    ).mode("append").save()

# ======== Запуск стріму =========
df_aggregated.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("update") \
    .option("checkpointLocation", "/tmp/new-checkpoint") \
    .option("failOnDataLoss", "false") \
    .start() \
    .awaitTermination()