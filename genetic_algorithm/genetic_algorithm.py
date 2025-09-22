from config.config import CONSUMER_CONFIG, MSG_WITH_EMBEDDINGS_TOPIC
from confluent_kafka import Consumer, KafkaError
import json

consumer = Consumer(CONSUMER_CONFIG)
consumer.subscribe([MSG_WITH_EMBEDDINGS_TOPIC])

batch = []
running = True

while running:
    try:
        event = consumer.poll(1.0)
        
        if not event:
            continue
        
        if event.error():
            if event.error().code() == KafkaError._PARTITION_EOF:
                print(f"Reached end of the partition: {event.topic()} [{event.partition()}]")
            else:
                print(f"Consumer error: {event.error()}")
            continue
        
        try:
            msg_decoded = json.loads(event.value().decode("utf-8"))      
        except(json.JSONDecodeError, KeyError) as e:
            print((f"Could not parse the JSON message: {e}. Message: {event.value()}"))
            continue
        
        print(msg_decoded)
        
    except KeyboardInterrupt:
        print("Stopping listener...")
    finally:
        print('Closing consumer...')
        consumer.close()