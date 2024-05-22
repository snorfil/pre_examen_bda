from pyspark.sql import SparkSession


def create_spark_session():
    aws_access_key_id = 'test'
    aws_secret_access_key = 'test'

    spark = SparkSession.builder \
        .appName("SPARK S3") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://spark-localstack-1:4566") \
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-hadoop-cloud_2.13:3.5.1,software.amazon.awssdk:s3:2.25.11") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.driver.extraClassPath", "/opt/spark/jars/s3-2.25.11.jar") \
        .config("spark.executor.extraClassPath", "/opt/spark/jars/s3-2.25.11.jar") \
        .master("spark://spark-master:7077") \
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
    spark = create_spark_session()

    empleados_df, clientes_df, reservas_df, menus_df, platos_df = read_data_from_s3(spark)

    empleados_df.show()
    clientes_df.show()
    reservas_df.show()
    menus_df.show()
    platos_df.show()

    spark.stop()


if __name__ == "__main__":
    main()
