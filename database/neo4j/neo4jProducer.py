from kafka import KafkaProducer
from neo4j import GraphDatabase
import json
import time

# Configurar la conexión con Neo4j
uri = "bolt://localhost:7687"
user = "neo4j"
password = "password"

driver = GraphDatabase.driver(uri, auth=(user, password))


def get_data_from_neo4j(tx):
    query = """
    MATCH (m:Menu)-[:INCLUYE]->(p:Plato)
    RETURN m, p
    """
    result = tx.run(query)
    data = []
    for record in result:
        data.append({
            "id_menu": record["id_menu"],
            "precio": record["precio"],
            "disponibilidad": record["disponibilidad"],
            "id_restaurante": record["id_restaurante"],
            "id_plato": record["id_plato"],
            "nombre": record["nombre"],
            "ingredientes": record["ingredientes"],
            "alergenos": record["alergenos"]
        })
    return data


def send_data_to_kafka(data, kafka_topic, interval):
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    for record in data:
        producer.send(kafka_topic, record)
        print(f'Sent: {record}')
        producer.flush()
        time.sleep(interval)

    print(f'Todos los datos han sido enviados al tópico Kafka {kafka_topic}')


if __name__ == "__main__":
    kafka_topic = "menus_stream"
    interval = 2

    with driver.session() as session:
        data = session.read_transaction(get_data_from_neo4j)

    send_data_to_kafka(data, kafka_topic, interval)
