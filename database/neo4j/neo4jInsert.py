from neo4j import GraphDatabase
import csv
import json

# Configurar la conexión con Neo4j
uri = "bolt://localhost:7687"
user = "neo4j"
password = "password"

driver = GraphDatabase.driver(uri, auth=(user, password))


def crear_nodos_y_relaciones(tx, menus, platos, relaciones):
    # Crear nodos de menú
    for menu in menus:
        tx.run(
            "CREATE (m:Menu {id_menu: $id_menu, precio: $precio, disponibilidad: $disponibilidad, id_restaurante: $id_restaurante})",
            id_menu=menu['id_menu'], precio=menu['precio'], disponibilidad=menu['disponibilidad'],
            id_restaurante=menu['id_restaurante'])

    # Crear nodos de plato
    for plato in platos:
        tx.run(
            "CREATE (p:Plato {id_plato: $id_plato, nombre: $nombre, ingredientes: $ingredientes, alergenos: $alergenos})",
            id_plato=plato['platoID'], nombre=plato['nombre'], ingredientes=plato['ingredientes'],
            alergenos=plato['alergenos'])

    # Crear relaciones entre menús y platos
    for relacion in relaciones:
        tx.run("MATCH (m:Menu {id_menu: $id_menu}), (p:Plato {id_plato: $id_plato}) CREATE (m)-[:INCLUYE]->(p)",
               id_menu=relacion['id_menu'], id_plato=relacion['id_plato'])


def leer_csv(filepath):
    with open(filepath, mode='r', encoding='utf-8-sig') as file:
        reader = csv.DictReader(file)
        return [row for row in reader]


def leer_json(filepath):
    with open(filepath, mode='r', encoding='utf-8') as file:
        return json.load(file)


if __name__ == "__main__":
    menus = leer_csv("../../data/neo4j/menu.csv")
    platos = leer_csv("../../data/neo4j/platos.csv")
    relaciones = leer_json("../../data/neo4j/relaciones.json")

    with driver.session() as session:
        session.write_transaction(crear_nodos_y_relaciones, menus, platos, relaciones)

    driver.close()
