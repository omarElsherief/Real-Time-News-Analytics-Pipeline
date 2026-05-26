from kafka import KafkaProducer
from newsapi import NewsApiClient
import json
import time
import re

KAFKA_BROKER = 'localhost:9092'
TOPIC = 'social-media'
API_KEY = '2701e5f273924ca0a12418d8786562ec'

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

newsapi = NewsApiClient(api_key=API_KEY)

print("Producer started. Fetching real news...")

BROAD_KEYWORDS = ['technology', 'software', 'data', 'computing']
FILTER_KEYWORDS = ['python', 'kafka', 'spark', 'AI', 'machine learning', 'big data']

while True:
    for broad in BROAD_KEYWORDS:
        try:
            response = newsapi.get_everything(
                q=broad,
                language='en',
                sort_by='publishedAt',
                page_size=20
            )
            articles = response.get('articles', [])
            sent_count = 0
            filtered_count = 0

            for article in articles:
                if not article['title'] or not article['description']:
                    continue

                combined = (article['title'] + ' ' + article['description']).lower()
                matched = [
                kw for kw in FILTER_KEYWORDS
                if re.search(rf"\b{kw.lower()}\b", combined)
                ]
                # matched = [kw for kw in FILTER_KEYWORDS if kw.lower() in combined]

                if not matched:
                    print(f"Filtered out [{broad}]: {article['title']}")
                    filtered_count += 1
                    continue

                message = {
                    "text": article['title'] + ' ' + article['description'],
                    "timestamp": article['publishedAt'],
                    "user": article['source']['name']
                }
                producer.send(TOPIC, message)
                producer.flush()
                sent_count += 1
                print(f"Sent [{broad} → {matched}]: {article['title']}")

            print(f"[{broad}] → {sent_count} sent, {filtered_count} filtered out of 20")

        except Exception as e:
            print(f"Error [{broad}]: {e}")

        print(f"Waiting 30 seconds before next keyword...")
        time.sleep(30)  

# while True:
#     for keyword in KEYWORDS:
#         try:
#             response = newsapi.get_everything(
#                 q=keyword,
#                 language='en',
#                 sort_by='publishedAt',
#                 page_size=2
#             )
#             articles = response.get('articles', [])
#             for article in articles:
#                 if article['title'] and article['description']:
#                     message = {
#                         "text": article['title'] + ' ' + article['description'],
#                         "timestamp": article['publishedAt'],
#                         "user": article['source']['name']
#                     }
#                     producer.send(TOPIC, message)
#                     print(f"Sent [{keyword}]: {article['title']}")
#         except Exception as e:
#             print(f"Error: {e}")

#     print("Waiting 60 seconds before next fetch...")
#     time.sleep(60)  # NewsAPI free tier has rate limits
