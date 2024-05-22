from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, StructField

# Definir el esquema de los datos
esquema = StructType([
    StructField("id_menu", StringType(), True),
    StructField("precio", StringType(), True),
    StructField("disponibilidad", StringType(), True),
    StructField("id_restaurante", StringType(), True),
    StructField("id_plato", StringType(), True),
    StructField("nombre", StringType(), True),
    StructField("ingredientes", StringType(), True),
    StructField("alergenos", StringType(), True)
])

# Crear sesión de Spark
spark = SparkSession.builder \
    .appName("KafkaSparkConsumer") \
    .config("spark.sql.shuffle.partitions", 4) \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.sql.streaming.schemaInference", "true") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:4566") \
    .config("spark.hadoop.fs.s3a.access.key", "test") \
    .config("spark.hadoop.fs.s3a.secret.key", "test") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .master("local[*]") \
    .getOrCreate()

# Leer el stream de Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "menus_stream") \
    .option("startingOffsets", "earliest") \
    .load()

# Convertir la columna 'value' a JSON y aplicar el esquema
df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), esquema).alias("data")) \
    .select("data.*")

# Mostrar el esquema del DataFrame para depuración
df.printSchema()

# Iniciar la computación
query = df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Esperar a que termine la consulta
query.awaitTermination()
