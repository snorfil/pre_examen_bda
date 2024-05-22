# segunda_ordinaria_BDA

## Creamos manualmente el topic accediendo al contenedor que alberga kafka
- docker ps
- docker exec -it <nombre_contenedor_kafka> /bin/bash
  - /opt/kafka/bin/kafka-topics.sh --create --topic clientes_stream --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
  - /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092


### Con el siguiente comando cambiando el --topic se vuelve consumidor para verificar el productor

kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic clientes_stream --from-beginning
