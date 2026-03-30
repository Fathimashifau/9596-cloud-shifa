import requests
from kafka import KafkaProducer
import json

# kafka producer
producer=KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
api_key="pub_379971d421944e53a2cf482947f8a633"
url=f"https://newsdata.io/api/1/news?apikey={api_key}&q=technology&language=en"

response=requests.get(url)
data=response.json()

articles=data.get("results",[])

for article in articles:
    producer.send("news-data", article)

producer.flush()

print("Articles sent to kafka")
