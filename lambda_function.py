import json
import requests
from kafka import KafkaProducer

def lambda_handler(event, context):

    print("Lambda started")

    producer = KafkaProducer(
        bootstrap_servers='13.60.202.80:9092',  # your EC2 public IP
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    api_key = "pub_e31bbd50144342e9b08db61aef990ceb"
    url = f"https://newsdata.io/api/1/news?apikey={api_key}&q=technology&language=en"

    response = requests.get(url)
    data = response.json()

    articles = data.get("results", [])

    print("Articles count:", len(articles))

    for article in articles:
        producer.send("news_topic", article)

    producer.flush()

    print("Data sent to Kafka")

    return {
        "statusCode": 200,
        "body": "Sent to Kafka"
    }

