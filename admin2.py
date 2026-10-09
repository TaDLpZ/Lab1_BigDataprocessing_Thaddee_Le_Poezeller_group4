# %%
from confluent_kafka.admin import AdminClient, NewTopic

# %%
config = {
  'bootstrap.servers': 'localhost:9092',
}

admin_client = AdminClient(config)

# %%
# Step 1: create a new topic for the book lines
topic = 'book'
futures = admin_client.create_topics(
  [NewTopic(topic, num_partitions=1, replication_factor=1)]
)

# Wait for Kafka to confirm (otherwise the listing below can come back empty)
for name, f in futures.items():
  try:
    f.result()
    print(f"Created topic: {name}")
  except Exception as e:
    print(f"Topic {name} not created: {e}")

# %%
x = admin_client.list_topics(timeout=10)
print("Topics on the broker:")
for t in x.topics.keys():
  print(" -", t)

# %%
# Uncomment to delete the topic and start over
#admin_client.delete_topics([topic])
