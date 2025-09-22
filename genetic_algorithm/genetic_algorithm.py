from config.config import CONSUMER_CONFIG, MSG_WITH_EMBEDDINGS_TOPIC, BATCH_SIZE
from confluent_kafka import Consumer, KafkaError
import json
import numpy

consumer = Consumer(CONSUMER_CONFIG)
consumer.subscribe([MSG_WITH_EMBEDDINGS_TOPIC])


messages_decoded = []
embeddings = []
running = True

try:
    while running:
        batch = consumer.consume(BATCH_SIZE, 5.0)
        
        if not batch:
            continue
        
        for event in batch:
            # Damaged events
            if event.error():
                if event.error().code() == KafkaError._PARTITION_EOF:
                    print(f"Reached end of the partition: {event.topic()} [{event.partition()}]")
                else:
                    print(f"Consumer error: {event.error()}")
                continue
            
            try:
                msg_decoded = json.loads(event.value().decode("utf-8"))
                messages_decoded.append(msg_decoded)
                embeddings.append(msg_decoded['embeddings'])
            except(json.JSONDecodeError, KeyError) as e:
                print((f"Could not parse the JSON message: {e}. Message: {event.value()}"))

        # Reduce the number of dimensions
        
        
        
        
except KeyboardInterrupt:
    print("Stopping listener...")
finally:
    print('Closing consumer...')
    consumer.close()