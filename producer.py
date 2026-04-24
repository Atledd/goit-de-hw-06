import json
import time
import random
from kafka import KafkaProducer
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

sensor_id = random.randint(1, 1000)

while True:
    data = {
        "sensor_id": str(sensor_id),
        "temperature": round(random.uniform(25, 45), 2),
        "humidity": round(random.uniform(15, 85), 2),
        "timestamp": datetime.utcnow().isoformat()
    }

    producer.send("building_sensors", data)
    print(data)

    time.sleep(2)
