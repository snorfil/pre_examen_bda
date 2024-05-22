from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, col, to_date, month, sum, explode, split

def read_from_postgres():
    spark = SparkSession.builder \
        .appName("DataAnalysis") \
        .config("spark.driver.extraClassPath", "/opt/spark-apps/driverS/postgresql-42.7.3.jar") \
        .master("spark://spark-master:7077") \
        .config("spark.jars", "postgresql-42.7.3.jar") \
        .getOrCreate()

    # Define connection properties
    jdbc_url = "jdbc:postgresql://spark-database-1:5432/PrimOrd"
    connection_properties = {
        "user": "primOrd",
        "password": "bdaPrimOrd",
        "driver": "org.postgresql.Driver"
    }

    try:
        # Leer datos desde las tablas de Postgres
        df_reservas = spark.read.jdbc(url=jdbc_url, table="reservas", properties=connection_properties)
        df_menus = spark.read.jdbc(url=jdbc_url, table="menus", properties=connection_properties)
        df_platos = spark.read.jdbc(url=jdbc_url, table="platos", properties=connection_properties)
        df_hoteles = spark.read.jdbc(url=jdbc_url, table="hoteles", properties=connection_properties)

        # Pregunta 1: ¿Cuáles son las preferencias alimenticias más comunes entre los clientes?
        preferencias_comida = df_reservas.groupBy("Preferencias Comida").count().orderBy(col("count").desc())
        print("Preferencias alimenticias más comunes entre los clientes:")
        preferencias_comida.show()

        # Pregunta 2: ¿Qué restaurante tiene el precio medio de menú más alto?
        precio_medio_menus = df_menus.groupBy("ID Restaurante").agg(avg("Precio").alias("Precio Medio")).orderBy(col("Precio Medio").desc())
        print("Restaurante con el precio medio de menú más alto:")
        precio_medio_menus.show(1)

        # Pregunta 3: ¿Existen tendencias en la disponibilidad de platos en los distintos restaurantes?
        tendencias_disponibilidad = df_platos.groupBy("ID Restaurante", "Disponibilidad").count().orderBy("ID Restaurante")
        print("Tendencias en la disponibilidad de platos en los distintos restaurantes:")
        tendencias_disponibilidad.show()

        # Pregunta 4: ¿Cuál es la duración media de la estancia de los clientes de un hotel?
        df_reservas = df_reservas.withColumn("Duracion Estancia", to_date(col("Fecha Salida"), "yyyy-MM-dd").cast("long") - to_date(col("Fecha Llegada"), "yyyy-MM-dd").cast("long"))
        duracion_media_estancia = df_reservas.groupBy("ID Hotel").agg(avg("Duracion Estancia").alias("Duracion Media"))
        print("Duración media de la estancia de los clientes de un hotel:")
        duracion_media_estancia.show()

        # Pregunta 5: ¿Existen periodos de máxima ocupación en función de las fechas de reserva?
        periodos_maxima_ocupacion = df_reservas.groupBy("Fecha Llegada").count().orderBy(col("count").desc())
        print("Periodos de máxima ocupación en función de las fechas de reserva:")
        periodos_maxima_ocupacion.show()

        # Pregunta 6: ¿Cuántos empleados tiene de media cada hotel?
        empleados_media = df_hoteles.withColumn("Num Empleados", size(split(col("empleados_ids"), ","))).agg(avg("Num Empleados").alias("Media Empleados"))
        print("Número medio de empleados por hotel:")
        empleados_media.show()

        # Pregunta 7: ¿Cuál es el índice de ocupación de cada hotel y varía según la categoría de habitación?
        indice_ocupacion = df_reservas.groupBy("ID Hotel", "Tipo Habitacion").count().orderBy("ID Hotel", "Tipo Habitacion")
        print("Índice de ocupación de cada hotel según la categoría de habitación:")
        indice_ocupacion.show()

        # Pregunta 8: ¿Podemos estimar los ingresos generados por cada hotel basándonos en los precios de las habitaciones y los índices de ocupación?
        df_habitaciones = spark.read.jdbc(url=jdbc_url, table="habitaciones", properties=connection_properties)
        ingresos_hoteles = df_reservas.join(df_habitaciones, df_reservas["Id Habitacion"] == df_habitaciones["numero_habitacion"]) \
            .groupBy("ID Hotel").agg(sum(col("tarifa_nocturna") * col("Duracion Estancia")).alias("Ingresos Estimados"))
        print("Ingresos estimados generados por cada hotel:")
        ingresos_hoteles.show()

        # Pregunta 9: ¿Qué platos son los más y los menos populares entre los restaurantes?
        platos_popularidad = df_reservas.groupBy("ID Plato").count().orderBy(col("count").desc())
        print("Platos más y menos populares entre los restaurantes:")
        platos_popularidad.show()

        # Pregunta 10: ¿Hay ingredientes o alérgenos comunes que aparezcan con frecuencia en los platos?
        df_platos = df_platos.withColumn("Ingredientes", explode(split(col("ingredientes"), ",")))
        ingredientes_frecuentes = df_platos.groupBy("Ingredientes").count().orderBy(col("count").desc())
        print("Ingredientes o alérgenos comunes en los platos:")
        ingredientes_frecuentes.show()

        # Pregunta 11: ¿Existen pautas en las preferencias de los clientes en función de la época del año?
        df_reservas = df_reservas.withColumn("Mes Llegada", month(col("Fecha Llegada")))
        pautas_preferencias = df_reservas.groupBy("Mes Llegada", "Preferencias Comida").count().orderBy("Mes Llegada", "Preferencias Comida")
        print("Pautas en las preferencias de los clientes en función de la época del año:")
        pautas_preferencias.show()

        # Pregunta 12: ¿Los clientes con preferencias dietéticas específicas tienden a reservar en restaurantes concretos?
        preferencias_restaurantes = df_reservas.groupBy("ID Restaurante", "Preferencias Comida").count().orderBy("ID Restaurante", "Preferencias Comida")
        print("Preferencias dietéticas específicas en restaurantes concretos:")
        preferencias_restaurantes.show()

        # Pregunta 13: ¿Existen discrepancias entre la disponibilidad de platos comunicada y las reservas reales realizadas?
        discrepancias_platos = df_reservas.groupBy("ID Plato").agg(count("ID Reserva").alias("Reservas Reales")).join(df_platos.groupBy("ID Plato").agg(sum("Disponibilidad").alias("Disponibilidad Comunicado")), "ID Plato").withColumn("Discrepancia", col("Disponibilidad Comunicado") - col("Reservas Reales"))
        print("Discrepancias entre la disponibilidad de platos comunicada y las reservas reales realizadas:")
        discrepancias_platos.show()

        # Pregunta 14: ¿Cómo se comparan los precios de las habitaciones de los distintos hoteles y existen valores atípicos?
        precios_habitaciones = df_habitaciones.groupBy("ID Hotel").agg(avg("tarifa_nocturna").alias("Precio Medio"))
        print("Comparación de los precios de las habitaciones de los distintos hoteles y valores atípicos:")
        precios_habitaciones.show()

    except Exception as e:
        print("Error reading data from PostgreSQL:", e)

    finally:
        # Stop SparkSession
        spark.stop()

if __name__ == "__main__":
    read_from_postgres()
