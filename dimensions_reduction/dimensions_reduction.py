from config.config import DR_CONSUMER_CONFIG, MSG_WITH_EMBEDDINGS_TOPIC, BATCH_SIZE, REDUCED_EMBEDDINGS, EMBEDDINGS, PRODUCER_CONFIG, MSG_WITH_REDUCED_EMBEDDINGS_TOPIC
from utils.utils import acked
from confluent_kafka import Consumer, KafkaError, Producer
import json
import numpy as np
from sklearn.decomposition import IncrementalPCA

consumer = Consumer(DR_CONSUMER_CONFIG)
consumer.subscribe([MSG_WITH_EMBEDDINGS_TOPIC])

producer = Producer(PRODUCER_CONFIG)


# To replace with constant
ipca = IncrementalPCA(n_components=64)

running = True

try:
    while running:
        batch = consumer.consume(BATCH_SIZE, 1.0)
        messages = []
        embeddings = []
        
        if not batch:
            continue
        
        for event in batch:
            if event.error():
                if event.error().code() == KafkaError._PARTITION_EOF:
                    print(f"Reached end of the partition: {event.topic()} [{event.partition()}]")
                else:
                    print(f"Consumer error: {event.error()}")
                continue
            
            try:
                event_decoded = json.loads(event.value().decode("utf-8"))
                messages.append(event_decoded)
                embeddings.append(event_decoded[EMBEDDINGS])
            except(json.JSONDecodeError, KeyError) as e:
                print((f"Could not parse the JSON message: {e}."))
                continue

        if not embeddings:
            continue
        
        # IPCA function
        try:
            embeddings_np = np.array(embeddings)
            ipca.partial_fit(embeddings_np)
            embeddings_reduced = ipca.transform(embeddings_np)
            print(f"Successfully processed a batch. Original shape: {embeddings_np.shape}, Reduced shape: {embeddings_reduced.shape}")
        
        except Exception as e:
            print(f"En exception {e} has occured!")
        
        for i, message in enumerate(messages):
            message[REDUCED_EMBEDDINGS] = embeddings_reduced[i].tolist()
            
            enriched_message_encoded = json.dumps(message).encode("utf-8")
            producer.produce(MSG_WITH_REDUCED_EMBEDDINGS_TOPIC, value=enriched_message_encoded, callback=acked)
            producer.poll(0)
            print("Sending messages from a batch...")

        producer.flush()          
            
        print("Committing offset...")
        consumer.commit(asynchronous=True)
        
except KeyboardInterrupt:
    print("Stopping listener...")
finally:
    print('Closing consumer...')
    consumer.close()