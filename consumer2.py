# %%
import re
import string
from collections import Counter
from confluent_kafka import Consumer

# %%
conf = {'bootstrap.servers': 'localhost:9092',
        'group.id': 'book-group',          # change this name to re-read the topic from the start
        'auto.offset.reset': 'earliest'}

consumer = Consumer(conf)

# %%
# Step 4: read from the new topic
topic = 'book'
consumer.subscribe([topic])

# %%
# Step 5: text cleaning (same idea as the word count lab)
STOP_WORDS = {
  'a', 'an', 'and', 'the', 'of', 'to', 'in', 'is', 'it', 'that', 'was',
  'he', 'she', 'i', 'you', 'his', 'her', 'with', 'as', 'for', 'on', 'at',
  'be', 'had', 'by', 'not', 'but', 'my', 'me', 'this', 'which', 'have',
  'from', 'or', 'they', 'were', 'so', 'him', 'all', 'we', 'are', 'been',
  'their', 'there', 'would', 'what', 'will', 'when', 'no', 'if', 'do',
  'said', 'one', 'them', 'could', 'any', 'more', 'than', 'very', 'such',
}

# string.punctuation misses the curly quotes and dashes Gutenberg books use
PUNCTUATION = string.punctuation + '“”‘’—–'
punct_table = str.maketrans(PUNCTUATION, ' ' * len(PUNCTUATION))

def clean_line(text):
  text = text.lower()                    # 1. lowercase
  text = text.translate(punct_table)     # 2. punctuation -> spaces
  text = re.sub(r'\d+', ' ', text)       # 3. remove numbers
  words = text.split()                   # 4. split into words
  return [w for w in words               # 5. drop stop words and single letters
          if w not in STOP_WORDS and len(w) > 1]

# %%
CLEAN_FILE = 'cleaned_book.txt'
COUNT_FILE = 'word_counts.txt'

MAX_EMPTY_POLLS = 10  # Ends after ~10 seconds of silence
MAX_ERRORS = 5        # Ends after 5 consecutive errors
empty_polls = 0
error_count = 0
received = 0
word_counts = Counter()

out = open(CLEAN_FILE, 'w', encoding='utf-8')

while True:
  msg = consumer.poll(1.0)

  # 1. Handle "No Message" (Timeout)
  if msg is None:
    empty_polls += 1
    if empty_polls >= MAX_EMPTY_POLLS:
      print("Closing: No new messages received.")
      break
    continue

  # 2. Handle Errors
  if msg.error():
    error_count += 1
    print(f"Consumer error: {msg.error()}")
    if error_count >= MAX_ERRORS:
      print("Closing: Too many consecutive errors.")
      break
    continue

  # 3. Handle Success
  empty_polls = 0
  error_count = 0
  received += 1

  words = clean_line(msg.value().decode('utf-8'))
  if words:
    out.write(' '.join(words) + '\n')    # cleaned line to the output file
    word_counts.update(words)            # running word count

  if received % 1000 == 0:
    print(f"Processed {received} lines...")

# Clean up
out.close()
consumer.close()

# %%
# Save the word counts, most frequent first
with open(COUNT_FILE, 'w', encoding='utf-8') as f:
  for word, count in word_counts.most_common():
    f.write(f"{word}\t{count}\n")

print(f"Lines received: {received}")
print(f"Cleaned text saved to: {CLEAN_FILE}")
print(f"Word counts saved to:  {COUNT_FILE} ({len(word_counts)} distinct words)")
print("Top 10 words:")
for word, count in word_counts.most_common(10):
  print(f"  {word}: {count}")
