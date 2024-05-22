from pyspark.sql import SparkSession
import sys

aws_access_key_id = 'test'
aws_secret_access_key = 'test'

ruta_restaurantes = "/opt/spark-data/json/restaurantes.json"
ruta_habitaciones = "/opt/spark-data/csv/habitaciones.csv"



spark = SparkSession.builder \
    .appName("SPARK S3") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://spark-localstack-1:4566") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.jars.packages","org.apache.spark:spark-hadoop-cloud_2.13:3.5.1,software.amazon.awssdk:s3:2.25.11") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.driver.extraClassPath", "/opt/spark/jars/s3-2.25.11.jar") \
    .config("spark.executor.extraClassPath", "/opt/spark/jars/s3-2.25.11.jar") \
    .master("spark://spark-master:7077") \
    .getOrCreate()


try:
    ## leer archivos
    df_restaurantes = spark.read.option("multiline", "true").json(ruta_restaurantes)
    df_habitaciones = spark.read.option("header", True).csv(ruta_habitaciones)

    # Mostrar el esquema de los DataFrames
    df_restaurantes.printSchema()
    df_habitaciones.printSchema()

    # Mostrar los datos leídos
    df_restaurantes.show(truncate=False)
    df_habitaciones.show(truncate=False)

    # Guardar los DataFrames en S3
    df_restaurantes \
        .write \
        .option('fs.s3a.committer.name', 'partitioned') \
        .option('fs.s3a.committer.staging.conflict-mode', 'replace') \
        .option("fs.s3a.fast.upload.buffer", "bytebuffer") \
        .mode('overwrite') \
        .json(path='s3a://hotel/restaurantes')

    df_habitaciones \
        .write \
        .option('fs.s3a.committer.name', 'partitioned') \
        .option('fs.s3a.committer.staging.conflict-mode', 'replace') \
        .option("fs.s3a.fast.upload.buffer", "bytebuffer")\
        .mode('overwrite') \
        .csv(path='s3a://hotel/habitaciones')

    spark.stop()

except Exception as e:
    print("Error leyendo o escribiendo los archivos JSON")
    print(e)
