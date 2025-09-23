from config.config import BATCH_SIZE, EMB_CONSUMER_CONFIG, EMBEDDING_MODEL, RAW_MSG_TOPIC, PRODUCER_CONFIG, MSG_WITH_EMBEDDINGS_TOPIC, TEXT, EMBEDDINGS
from utils.utils import acked
from confluent_kafka import Consumer, KafkaError, Producer
import json
from sentence_transformers import SentenceTransformer

consumer = Consumer(EMB_CONSUMER_CONFIG)
consumer.subscribe([RAW_MSG_TOPIC])

producer = Producer(PRODUCER_CONFIG)

model = SentenceTransformer(EMBEDDING_MODEL)

running = True

try:
    while running:
        batch = consumer.consume(BATCH_SIZE, 1.0)
        messages = []
        
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
                event_decoded = json.loads(event.value().decode("utf-8"))[TEXT]
                messages.append(event_decoded)
            except(json.JSONDecodeError, KeyError) as e:
                print((f"Could not parse the JSON message: {e}."))
                continue

        print("Encoding messages with SentenceTransformer...")
        embeddings = model.encode(messages)
        print(f"Successfully created {len(embeddings)} embeddings.")

        # Send messages to next topic
        for i, text in enumerate(messages):

            enriched_event = {
                TEXT: text,
                EMBEDDINGS: embeddings[i].tolist()
            }
            
            enriched_event_encoded = json.dumps(enriched_event).encode("utf-8")
            producer.produce(MSG_WITH_EMBEDDINGS_TOPIC, value=enriched_event_encoded, callback=acked)
            producer.poll(0)
            print("Sending messages from a batch...")
            
            producer.flush()          
            print("All messages from a batch have been sent...")
            
            print("Committing offset...")
            consumer.commit(asynchronous=True)

except KeyboardInterrupt:
    print("Stopping listener...")
finally:
    print('Closing consumer...')
    consumer.close()
