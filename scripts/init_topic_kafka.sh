#!/bin/bash


cub kafka-ready -b kafka:9092 1 20

kafka-topics --create --topic clientes_stream --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1
kafka-topics --create --topic menus_stream --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1
kafka-topics --create --topic reservas_stream --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1


echo "todos los topicos creados"
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic clientes_stream --from-beginning
