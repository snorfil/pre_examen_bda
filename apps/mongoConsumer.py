from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StringType, StructField, IntegerType

aws_access_key_id = 'test'
aws_secret_access_key = 'test'

spark = SparkSession.builder \
    .appName("Streaming from Kafka") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.sql.shuffle.partitions", 2) \
    .config("spark.hadoop.fs.s3a.endpoint", "http://spark-localstack-1:4566") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
            "org.apache.kafka:kafka-clients:2.8.0,"
            "software.amazon.awssdk:s3:2.25.11,"
            "org.apache.commons:commons-pool2:2.11.1,"
            "org.apache.kafka:kafka_2.12:2.8.0") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.driver.extraClassPath", "/opt/spark/jars/*") \
    .config("spark.executor.extraClassPath", "/opt/spark/jars/*") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Leer el stream de Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9093") \
    .option("subscribe", "clientes_stream") \
    .load()

# Definir el esquema de los datos
esquema = StructType([
    StructField("_id", StringType(), True),
    StructField("id_cliente", IntegerType(), True),
    StructField("nombre", StringType(), True),
    StructField("direccion", StringType(), True),
    StructField("preferencias_alimenticias", StringType(), True)
])

# Convertir la columna 'value' a JSON y aplicar el esquema
df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", esquema).alias("data")) \
    .select("data.*")

# Mostrar el esquema del DataFrame para depuración
df.printSchema()

# Iniciar la computación
# query = df \
#     .writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .start()

query = df \
    .writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "s3a://hotel/csv/clientes") \
    .option("checkpointLocation", "s3a://hotel/logs/clientes") \
    .option("header", "true") \
    .start()

# Esperar a que termine la consulta
query.awaitTermination()
