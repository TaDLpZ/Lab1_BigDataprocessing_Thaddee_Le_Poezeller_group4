# %%
import socket
from confluent_kafka import Producer

# %%
conf = {'bootstrap.servers': 'localhost:9092',
        'client.id': socket.gethostname()}

producer = Producer(conf)

# %%
topic = 'book'
book_file = 'book.txt'   # the Project Gutenberg plain-text file (UTF-8)

# %% Read the book line by line and send each line to the topic
sent = 0
in_book = False

# encoding='utf-8-sig' is important on Windows (default there is not UTF-8)
with open(book_file, encoding='utf-8-sig') as f:
  for line in f:
    # Gutenberg files have a licence header and footer around the real book.
    # Only send what is between the START and END markers.
    if line.startswith('***') and 'START OF' in line:
      in_book = True
      continue
    if line.startswith('***') and 'END OF' in line:
      break
    if not in_book:
      continue

    line = line.strip()
    if line == '':
      continue   # skip empty lines

    producer.produce(topic=topic, value=line)
    sent += 1
    producer.poll(0)   # lets the library send in the background

    if sent % 1000 == 0:
      print(f"Sent {sent} lines...")

# %%
producer.flush()   # wait until every line has reached Kafka
print(f"Done: sent {sent} lines to topic '{topic}'")
