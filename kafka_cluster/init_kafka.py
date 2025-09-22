from config.config import INIT_CLIENT_CONFIG, RAW_MSG_TOPIC, MSG_WITH_EMBEDDINGS_TOPIC, PARTITIONS_NUM, REPLICATION_FACTOR_NUM
from confluent_kafka.admin import AdminClient, NewTopic

admin_client = AdminClient(INIT_CLIENT_CONFIG)

topics_to_create = [
    NewTopic(topic=RAW_MSG_TOPIC, num_partitions=PARTITIONS_NUM, replication_factor=REPLICATION_FACTOR_NUM),
    NewTopic(topic=MSG_WITH_EMBEDDINGS_TOPIC, num_partitions=PARTITIONS_NUM, replication_factor=REPLICATION_FACTOR_NUM),
]

futures = admin_client.create_topics(topics_to_create)

for topic, f in futures.items():
    try:
        f.result()  
        print(f"Topic '{topic}' has been created.")
    except Exception as e:
        print(f"An error has occured whilst creating {topic}: {e}")