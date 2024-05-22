import json
import time
from kafka import KafkaProducer

# Configuración del productor Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

with open('../../data/kafka/reservas.txt', 'r') as file:
    reservas = file.read().split('*** Reserva ')
    for reserva in reservas[1:]:  # Ignorar la primera división que es vacía
        lines = reserva.strip().split('\n')
        reserva_id = lines[0].replace(' ***', '')
        reserva_data = {line.split(': ')[0].strip(): line.split(': ')[1].strip() for line in lines[1:]}
        reserva_data['ID Reserva'] = reserva_id
        producer.send('reservas_topic', reserva_data)
        print(f"Enviado: {reserva_data}")
        time.sleep(2)  # Esperar 2 segundos antes de enviar el siguiente mensaje

producer.flush()
producer.close()
