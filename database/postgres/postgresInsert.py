import psycopg2
import csv

def create_tables(conn):
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS empleados (
            id_empleado SERIAL PRIMARY KEY,
            nombre VARCHAR(255),
            puesto VARCHAR(255),
            fecha_contratacion DATE
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS hoteles (
            id_hotel SERIAL PRIMARY KEY,
            nombre VARCHAR(255),
            direccion VARCHAR(255),
            id_empleados TEXT
        );
        """)
        conn.commit()

def insert_data_empleados(conn, file_path):
    with conn.cursor() as cur:
        with open(file_path, 'r') as f:
            reader = csv.reader(f)
            next(reader)  # Skip header row
            for row in reader:
                cur.execute(
                    "INSERT INTO empleados (nombre, puesto, fecha_contratacion) VALUES (%s, %s, %s)",
                    row[1:]
                )
    conn.commit()

def insert_data_hoteles(conn, file_path):
    with conn.cursor() as cur:
        with open(file_path, 'r') as f:
            reader = csv.reader(f)
            next(reader)  # Skip header row
            for row in reader:
                cur.execute(
                    "INSERT INTO hoteles (nombre, direccion, id_empleados) VALUES (%s, %s, %s)",
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
        insert_data_empleados(conn, '../../data/postgres/empleados.csv')
        insert_data_hoteles(conn, '../../data/postgres/hoteles.csv')
    finally:
        conn.close()

if __name__ == "__main__":
    main()

