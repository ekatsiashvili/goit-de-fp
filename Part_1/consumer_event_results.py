from kafka import KafkaConsumer
from dotenv import load_dotenv
import json
import os

load_dotenv()

# Конфігурація Kafka
kafka_config = {
    "bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS").split(","),
    "security_protocol": os.getenv("KAFKA_SECURITY_PROTOCOL"),
    "kafka_event_topic": os.getenv("KAFKA_EVENT_TOPIC"), 
    "kafka_output_topic": os.getenv("KAFKA_OUTPUT_TOPIC")
}

consumer_event_results = KafkaConsumer(
    kafka_config['kafka_event_topic'],
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda k: k.decode('utf-8') if k else None,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='event_results_reader'
)

print("Listening for data...\n")
for msg in consumer_event_results:
    print(f"[{msg.partition}] Athlete_event_result - {msg.key}:\n{msg.value}\n")
