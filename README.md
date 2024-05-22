# segunda_ordinaria_BDA

## Creamos manualmente el topic accediendo al contenedor que alberga kafka
- docker ps
- docker exec -it <nombre_contenedor_kafka> /bin/bash
  - /opt/kafka/bin/kafka-topics.sh --create --topic clientes_stream --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
  - /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092


### Con el siguiente comando cambiando el --topic se vuelve consumidor para verificar el productor

kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic clientes_stream --from-beginning

Para ejecutar un map_reduce

docker cp reservations_count.pig hadoop-namenode:/reservations_count.pig
docker exec -it hadoop-namenode bash
pig reservations_count.pig

javac -classpath `hadoop classpath` -d reservations_classes ReservationsCount.java
jar -cvf reservations.jar -C reservations_classes/ .

docker cp ReservationsCount.jar hadoop-namenode:/ReservationsCount.jar
docker exec -it hadoop-namenode bash
hadoop jar ReservationsCount.jar ReservationsCount /path/to/input /path/to/output


hadoop jar reservations.jar ReservationsCount /path/to/reservas.txt /path/to/output