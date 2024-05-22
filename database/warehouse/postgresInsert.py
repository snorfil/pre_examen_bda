import csv
import json
import psycopg2


def create_tables(conn):
    with conn.cursor() as cur:
        # Crear tabla clientes
        cur.execute("""
        CREATE TABLE IF NOT EXISTS clientes (
            id_cliente INTEGER PRIMARY KEY,
            nombre VARCHAR(255),
            direccion VARCHAR(255),
            preferencias_dieteticas VARCHAR(255)
        );
        """)

        # Crear tabla reservas
        cur.execute("""
        CREATE TABLE IF NOT EXISTS reservas (
            id_reserva SERIAL PRIMARY KEY,
            id_cliente INTEGER,
            fecha_llegada DATE,
            fecha_salida DATE,
            tipo_habitacion VARCHAR(255),
            preferencias_comida VARCHAR(255),
            id_habitacion INTEGER,
            id_restaurante INTEGER
        );
        """)

        # Crear tabla menus
        cur.execute("""
        CREATE TABLE IF NOT EXISTS menus (
            id_menu INTEGER PRIMARY KEY,
            precio DECIMAL,
            disponibilidad BOOLEAN,
            id_restaurante INTEGER
        );
        """)

        # Crear tabla platos
        cur.execute("""
        CREATE TABLE IF NOT EXISTS platos (
            id_plato SERIAL PRIMARY KEY,
            nombre VARCHAR(255),
            ingredientes VARCHAR(255),
            alergenos VARCHAR(255)
        );
        """)

        conn.commit()


def insert_clientes(conn, file_path):
    with conn.cursor() as cur:
        with open(file_path, 'r') as f:
            clientes = json.load(f)
            for cliente in clientes:
                cur.execute(
                    "INSERT INTO clientes (id_cliente, nombre, direccion, preferencias_dieteticas) VALUES (%s,%s, %s, %s)",
                    (cliente['id_cliente'], cliente['nombre'], cliente['direccion'],
                     cliente['preferencias_alimenticias'])
                )
    conn.commit()


def insert_reservas(conn, file_path):
    with conn.cursor() as cur:
        with open(file_path, 'r') as f:
            reserva_data = f.read().split('*** Reserva')
            for reserva in reserva_data[1:]:  # Skip the first empty split part
                lines = reserva.splitlines()
                id_cliente = int(lines[1].split(": ")[1])
                fecha_llegada = lines[2].split(": ")[1]
                fecha_salida = lines[3].split(": ")[1]
                tipo_habitacion = lines[4].split(": ")[1]
                preferencias_comida = lines[5].split(": ")[1]
                id_habitacion = int(lines[6].split(": ")[1])
                id_restaurante = int(lines[7].split(": ")[1])

                cur.execute(
                    "INSERT INTO reservas (id_cliente, fecha_llegada, fecha_salida, tipo_habitacion, preferencias_comida, id_habitacion, id_restaurante) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (id_cliente, fecha_llegada, fecha_salida, tipo_habitacion, preferencias_comida, id_habitacion,
                     id_restaurante)
                )
    conn.commit()


def insert_menus(conn, file_path):
    with conn.cursor() as cur:
        with open(file_path, 'r') as f:
            reader = csv.reader(f)
            next(reader)  # Skip header row
            for row in reader:
                cur.execute(
                    "INSERT INTO menus (id_menu, precio, disponibilidad,id_restaurante) VALUES (%s, %s, %s, %s)",
                    (row[0], row[1], bool(row[2]), row[3])
                )
    conn.commit()


def insert_platos(conn, file_path):
    with conn.cursor() as cur:
        with open(file_path, 'r') as f:
            reader = csv.reader(f)
            next(reader)  # Skip header row
            for row in reader:
                cur.execute(
                    "INSERT INTO platos (nombre, ingredientes, alergenos) VALUES (%s, %s, %s)",
                    (row[1], row[2], row[3])
                )
    conn.commit()


def main():
    conn = psycopg2.connect(
        dbname="PrimOrd",
        user="primOrd",
        password="bdaPrimOrd",
        host="localhost",
        port="9999"
    )

    try:
        create_tables(conn)
        insert_clientes(conn, '../../data/mongo/clientes.json')
        insert_reservas(conn, '../../data/kafka/reservas.txt')
        insert_menus(conn, '../../data/neo4j/menu.csv')
        insert_platos(conn, '../../data/neo4j/platos.csv')
    finally:
        conn.close()


if __name__ == "__main__":
    main()
