from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, col, to_date, month, sum, explode, split


def create_spark_session():
    spark = SparkSession.builder \
        .appName("PostgresAnalysis") \
        .config("spark.driver.extraClassPath", "/path/to/postgresql-42.7.3.jar") \
        .getOrCreate()
    return spark

def read_table(spark, table_name):
    jdbc_url = "jdbc:postgresql://localhost:9999/PrimOrd"
    connection_properties = {
        "user": "primOrd",
        "password": "bdaPrimOrd",
        "driver": "org.postgresql.Driver"
    }

    df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=connection_properties)
    return df


def load_dataframes(spark):
    empleados_df = read_table(spark, "empleados")
    clientes_df = read_table(spark, "clientes")
    reservas_df = read_table(spark, "reservas")
    menus_df = read_table(spark, "menus")
    platos_df = read_table(spark, "platos")

    return empleados_df, clientes_df, reservas_df, menus_df, platos_df


def preferencias_comunes(clientes_df):
    preferencias_df = clientes_df.groupBy("preferencias_dieteticas").count().orderBy("count", ascending=False)
    preferencias_df.show()


def restaurante_precio_medio(menus_df):
    precio_medio_df = menus_df.groupBy("id_restaurante").avg("precio").orderBy("avg(precio)", ascending=False)
    precio_medio_df.show()


def disponibilidad_platos(menus_df):
    disponibilidad_df = menus_df.groupBy("id_restaurante", "disponibilidad").count().orderBy("id_restaurante",
                                                                                             "disponibilidad")
    disponibilidad_df.show()


from pyspark.sql.functions import col, datediff


def duracion_estancia(reservas_df):
    reservas_df = reservas_df.withColumn("duracion", datediff(col("fecha_salida"), col("fecha_llegada")))
    duracion_media_df = reservas_df.groupBy("id_hotel").avg("duracion")
    duracion_media_df.show()


def periodos_maxima_ocupacion(reservas_df):
    ocupacion_df = reservas_df.groupBy("fecha_llegada").count().orderBy("count", ascending=False)
    ocupacion_df.show()


def empleados_por_hotel(hoteles_df):
    from pyspark.sql.functions import size
    hoteles_df = hoteles_df.withColumn("num_empleados", size(col("id_empleados")))
    empleados_media_df = hoteles_df.agg({"num_empleados": "avg"})
    empleados_media_df.show()


def indice_ocupacion(reservas_df):
    ocupacion_df = reservas_df.groupBy("id_hotel", "tipo_habitacion").count().orderBy("id_hotel", "tipo_habitacion")
    ocupacion_df.show()


def ingresos_por_hotel(reservas_df, menus_df):
    ingresos_df = reservas_df.join(menus_df, reservas_df.id_restaurante == menus_df.id_restaurante, "left") \
        .groupBy("id_hotel").agg({"precio": "sum"})
    ingresos_df.show()


def popularidad_platos(menus_df, platos_df):
    popularidad_df = menus_df.join(platos_df, menus_df.id_plato == platos_df.id_plato, "left") \
        .groupBy("nombre_plato").count().orderBy("count", ascending=False)
    popularidad_df.show()


def ingredientes_comunes(platos_df):
    ingredientes_df = platos_df.groupBy("ingredientes").count().orderBy("count", ascending=False)
    ingredientes_df.show()

def preferencias_estacionales(reservas_df, clientes_df):
    estacionales_df = reservas_df.join(clientes_df, reservas_df.id_cliente == clientes_df.id_cliente, "left") \
                                 .groupBy("preferencias_dieteticas", "fecha_llegada").count().orderBy("count", ascending=False)
    estacionales_df.show()

def preferencias_restaurante(clientes_df, reservas_df):
    preferencias_restaurante_df = reservas_df.join(clientes_df, reservas_df.id_cliente == clientes_df.id_cliente, "left") \
                                             .groupBy("id_restaurante", "preferencias_dieteticas").count().orderBy("count", ascending=False)
    preferencias_restaurante_df.show()

def discrepancias_platos(menus_df, reservas_df):
    discrepancias_df = menus_df.join(reservas_df, menus_df.id_restaurante == reservas_df.id_restaurante, "left") \
                               .groupBy("id_restaurante", "disponibilidad").agg({"id_reserva": "count"})
    discrepancias_df.show()

def comparacion_precios(hoteles_df, menus_df):
    comparacion_df = hoteles_df.join(menus_df, hoteles_df.id_hotel == menus_df.id_restaurante, "left") \
                               .groupBy("nombre_hotel").agg({"precio": "avg"}).orderBy("avg(precio)", ascending=False)
    comparacion_df.show()


def main():
    spark = create_spark_session()

    # Cargar DataFrames
    empleados_df, clientes_df, reservas_df, menus_df, platos_df = load_dataframes(spark)

    # 1. Análisis de las preferencias de los clientes
    print("Análisis de las preferencias de los clientes:")
    preferencias_comunes(clientes_df)

    # 2. Análisis del rendimiento del restaurante
    print("Análisis del rendimiento del restaurante:")
    restaurante_precio_medio(menus_df)
    disponibilidad_platos(menus_df)

    # 3. Patrones de reserva
    print("Patrones de reserva:")
    duracion_estancia(reservas_df)
    periodos_maxima_ocupacion(reservas_df)

    # 4. Gestión de empleados
    print("Gestión de empleados:")
    empleados_por_hotel(empleados_df)

    # 5. Ocupación e ingresos del hotel
    print("Ocupación e ingresos del hotel:")
    indice_ocupacion(reservas_df)
    ingresos_por_hotel(reservas_df, menus_df)

    # 6. Análisis de menús
    print("Análisis de menús:")
    popularidad_platos(menus_df, platos_df)
    ingredientes_comunes(platos_df)

    # 7. Comportamiento de los clientes
    print("Comportamiento de los clientes:")
    preferencias_estacionales(reservas_df, clientes_df)
    preferencias_restaurante(clientes_df, reservas_df)

    # 8. Garantía de calidad
    print("Garantía de calidad:")
    discrepancias_platos(menus_df, reservas_df)

    # 9. Análisis de mercado
    print("Análisis de mercado:")
    comparacion_precios(empleados_df, menus_df)

    spark.stop()


if __name__ == "__main__":
    main()