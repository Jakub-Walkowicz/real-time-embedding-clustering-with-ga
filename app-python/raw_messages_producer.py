from config import PRODUCER_CONFIG, RAW_MSG_TOPIC, DATASET_PARAMS
from utils import acked
from confluent_kafka import Producer
import pandas as pd
import json
import time

producer = Producer(PRODUCER_CONFIG)

dataframe = pd.read_csv(
    **DATASET_PARAMS
)

for row in dataframe.to_dict(orient="records"):
    event_encoded = json.dumps(row).encode("utf-8")
    producer.produce(RAW_MSG_TOPIC, value=event_encoded, callback=acked)
    producer.poll(0)
    
    # Real-time API simulation
    time.sleep(0.05)

print("Sending messages...")
producer.flush()
print("All messages have been sent.")
