from config.config import GA_CONSUMER_CONFIG, MSG_WITH_EMBEDDINGS_TOPIC, BATCH_SIZE
from confluent_kafka import Consumer, KafkaError
import json
import numpy as np
from sklearn.decomposition import IncrementalPCA

consumer = Consumer(GA_CONSUMER_CONFIG)
consumer.subscribe([MSG_WITH_EMBEDDINGS_TOPIC])

# To replace with constant
ipca = IncrementalPCA(n_components=64)

messages = []
running = True
embeddings = []


try:
    while running:
        batch = consumer.consume(BATCH_SIZE, 5.0)
        
        print(len(batch))
        exit
        
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
                messages.append(msg_decoded)
                embeddings.append(msg_decoded['embedding'])
            except(json.JSONDecodeError, KeyError) as e:
                print((f"Could not parse the JSON message: {e}. Message: {event.value()}"))

        if not embeddings:
            continue
        
        # Reduce the number of dimensions
        embeddings_batch = np.array(embeddings)
        
        try:
            # Incrementally fit the model with real-time data
            ipca.partial_fit(embeddings_batch)
            reduced_embeddings = ipca.transform(embeddings)
            print(f"Successfully processed a batch. Original shape: {embeddings_batch.shape}, Reduced shape: {reduced_embeddings.shape}")
        except Exception as e:
            print("En exception {e} has occured!")
        
        for i, message in enumerate(messages):
            message['reduced_embeddings'] = reduced_embeddings[i].tolist()
        
except KeyboardInterrupt:
    print("Stopping listener...")
finally:
    print('Closing consumer...')
    consumer.close()