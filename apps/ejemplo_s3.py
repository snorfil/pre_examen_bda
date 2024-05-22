import boto3
import botocore
import os
from pyspark.sql import SparkSession


def create_s3_buckets():
    s3 = boto3.client('s3', endpoint_url='http://localhost:4566')

    buckets = ['empleados-bucket', 'clientes-bucket', 'reservas-bucket', 'menus-bucket', 'platos-bucket']

    for bucket in buckets:
        try:
            s3.create_bucket(Bucket=bucket)
            print(f'Bucket {bucket} creado.')
        except botocore.exceptions.ClientError as e:
            print(f'Error al crear el bucket {bucket}: {e}')


def upload_files_to_s3():
    s3 = boto3.client('s3', endpoint_url='http://localhost:4566')

    files = {
        'empleados.csv': 'empleados-bucket',
        'clientes.json': 'clientes-bucket',
        'reservas.txt': 'reservas-bucket',
        'menu.csv': 'menus-bucket',
        'platos.csv': 'platos-bucket'
    }

    for file_name, bucket in files.items():
        file_path = os.path.join('/path/to/data', file_name)
        with open(file_path, 'rb') as file_data:
            s3.upload_fileobj(file_data, bucket, file_name)
            print(f'{file_name} subido a {bucket}.')


def create_spark_session():
    spark = SparkSession.builder \
        .appName("S3Integration") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:4566") \
        .config("spark.hadoop.fs.s3a.access.key", "dummy") \
        .config("spark.hadoop.fs.s3a.secret.key", "dummy") \
        .config("spark.hadoop.fs.s3a.path.style.access", True) \
        .getOrCreate()
    return spark


def read_data_from_s3(spark):
    empleados_df = spark.read.csv("s3a://empleados-bucket/empleados.csv", header=True, inferSchema=True)
    clientes_df = spark.read.json("s3a://clientes-bucket/clientes.json")
    reservas_df = spark.read.text("s3a://reservas-bucket/reservas.txt")
    menus_df = spark.read.csv("s3a://menus-bucket/menu.csv", header=True, inferSchema=True)
    platos_df = spark.read.csv("s3a://platos-bucket/platos.csv", header=True, inferSchema=True)

    return empleados_df, clientes_df, reservas_df, menus_df, platos_df


def main():
    # Crear buckets en S3
    create_s3_buckets()

    # Subir archivos a S3
    upload_files_to_s3()

    # Crear sesión de Spark
    spark = create_spark_session()

    # Leer datos desde S3
    empleados_df, clientes_df, reservas_df, menus_df, platos_df = read_data_from_s3(spark)

    # Mostrar los DataFrames
    empleados_df.show()
    clientes_df.show()
    reservas_df.show()
    menus_df.show()
    platos_df.show()

    spark.stop()


if __name__ == "__main__":
    main()
