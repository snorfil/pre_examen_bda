from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DateType

# Inicializar SparkSession
spark = SparkSession.builder \
    .appName("Kafka Consumer from kafka") \
    .getOrCreate()

# Definir el esquema para los datos JSON
schema = StructType([
    StructField("ID Cliente", StringType(), True),
    StructField("Fecha Llegada", StringType(), True),
    StructField("Fecha Salida", StringType(), True),
    StructField("Tipo Habitacion", StringType(), True),
    StructField("Preferencias Comida", StringType(), True),
    StructField("Id Habitacion", StringType(), True),
    StructField("ID Restaurante", StringType(), True),
    StructField("ID Reserva", StringType(), True)
])

# Leer datos desde Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "reservas_topic") \
    .load()

# Convertir los datos de Kafka de bytes a string y aplicar el esquema
df_reservas = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Mostrar datos en la consola (para debugging)
query = df_reservas.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
