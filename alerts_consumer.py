import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "temperature_alerts_artem",
    "humidity_alerts_artem",
    bootstrap_servers='127.0.0.1:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

for message in consumer:
    print("ALERT:", message.value)
