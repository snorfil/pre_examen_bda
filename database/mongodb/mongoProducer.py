from kafka import KafkaProducer
import pymongo
import json
import time


def mongo_to_kafka(mongo_db, mongo_collection, kafka_topic, interval):
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client[mongo_db]
    collection = db[mongo_collection]

    producer = KafkaProducer(bootstrap_servers='localhost:9092',
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    for doc in collection.find():
        doc['_id'] = str(doc['_id'])
        producer.send(kafka_topic, doc)
        print(f'Sent: {doc}')
        producer.flush()
        time.sleep(interval)
    print(f'All data sent to Kafka topic {kafka_topic}')


if __name__ == "__main__":
    mongo_to_kafka('bda', 'clientes', 'clientes_stream', 2)
