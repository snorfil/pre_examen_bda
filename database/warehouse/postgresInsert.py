import json

import psycopg2

def create_tables(conn):
    with conn.cursor() as cur:
        # Crear tabla empleados
        cur.execute("""
        CREATE TABLE IF NOT EXISTS empleados (
            id_empleado SERIAL PRIMARY KEY,
            nombre VARCHAR(255),
            puesto VARCHAR(255),
            fecha_contratacion DATE
        );
        """)

        # Crear tabla combinada de clientes y reservas
        cur.execute("""
        CREATE TABLE IF NOT EXISTS clientes_reservas (
            id_reserva SERIAL PRIMARY KEY,
            id_cliente SERIAL,
            nombre_cliente VARCHAR(255),
            direccion_cliente VARCHAR(255),
            preferencias_dieteticas VARCHAR(255),
            fecha_llegada DATE,
            fecha_salida DATE,
            tipo_habitacion VARCHAR(255),
            preferencias_comida VARCHAR(255),
            id_habitacion INTEGER,
            id_restaurante INTEGER
        );
        """)

        # Crear tabla combinada de menús y platos
        cur.execute("""
        CREATE TABLE IF NOT EXISTS menus_platos (
            id_menu SERIAL PRIMARY KEY,
            id_restaurante INTEGER,
            precio DECIMAL,
            disponibilidad BOOLEAN,
            id_plato SERIAL,
            nombre_plato VARCHAR(255),
            ingredientes VARCHAR(255),
            alergenos VARCHAR(255)
        );
        """)

        conn.commit()


def insert_empleados(conn, file_path):
    with conn.cursor() as cur:
        with open(file_path, 'r') as f:
            reader = csv.reader(f)
            next(reader)  # Skip header row
            for row in reader:
                cur.execute(
                    "INSERT INTO empleados (nombre, puesto, fecha_contratacion) VALUES (%s, %s, %s)",
                    (row[1], row[2], row[3])
                )
    conn.commit()


def insert_clientes_reservas(conn, clientes_path, reservas_path):
    # Insertar datos de clientes
    with conn.cursor() as cur:
        with open(clientes_path, 'r') as f:
            clientes = json.load(f)
            for cliente in clientes:
                cur.execute(
                    "INSERT INTO clientes_reservas (id_cliente, nombre_cliente, direccion_cliente, preferencias_dieteticas) VALUES (%s, %s, %s, %s)",
                    (cliente['id_cliente'], cliente['nombre'], cliente['direccion'], cliente['preferencias_dieteticas'])
                )

    # Insertar datos de reservas
    with conn.cursor() as cur:
        with open(reservas_path, 'r') as f:
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
                    "INSERT INTO clientes_reservas (id_cliente, fecha_llegada, fecha_salida, tipo_habitacion, preferencias_comida, id_habitacion, id_restaurante) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (id_cliente, fecha_llegada, fecha_salida, tipo_habitacion, preferencias_comida, id_habitacion,
                     id_restaurante)
                )
    conn.commit()


def insert_menus_platos(conn, menus_path, platos_path):
    with conn.cursor() as cur:
        # Insertar datos de menús
        with open(menus_path, 'r') as f:
            reader = csv.reader(f)
            next(reader)  # Skip header row
            for row in reader:
                cur.execute(
                    "INSERT INTO menus_platos (id_menu, id_restaurante, precio, disponibilidad) VALUES (%s, %s, %s, %s)",
                    (row[0], row[1], row[2], row[3])
                )

        # Insertar datos de platos
        with open(platos_path, 'r') as f:
            reader = csv.reader(f)
            next(reader)  # Skip header row
            for row in reader:
                cur.execute(
                    "INSERT INTO menus_platos (id_plato, nombre_plato, ingredientes, alergenos) VALUES (%s, %s, %s, %s)",
                    (row[0], row[1], row[2], row[3])
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
        insert_empleados(conn, '../../data/postgres/empleados.csv')
        insert_clientes_reservas(conn, '/mnt/data/clientes.json', '/mnt/data/reservas.txt')
        insert_menus_platos(conn, '/mnt/data/menu.csv', '/mnt/data/platos.csv')
        print("Data inserted successfully.")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
