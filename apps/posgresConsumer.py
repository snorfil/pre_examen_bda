from pyspark.sql import SparkSession


def read_from_postgres():
    spark = SparkSession.builder \
        .appName("ReadFromPostgres") \
        .config("spark.driver.extraClassPath", "/opt/spark-apps/drivers/postgresql-42.7.3.jar") \
        .master("spark://spark-master:7077") \
        .config("spark.jars", "postgresql-42.7.3.jar") \
        .getOrCreate()

    # Define connection properties
    jdbc_url = "jdbc:postgresql://postgres:9999/PrimOrd"
    connection_properties = {
        "user": "primOrd",
        "password": "bdaPrimOrd",
        "driver": "org.postgresql.Driver"
    }

    # Define table names
    empleados_table = "empleados"
    hoteles_table = "hoteles"

    try:
        # Read data from PostgreSQL tables into DataFrames
        empleados_df = spark.read.jdbc(url=jdbc_url, table=empleados_table, properties=connection_properties)
        hoteles_df = spark.read.jdbc(url=jdbc_url, table=hoteles_table, properties=connection_properties)

        # Show the DataFrames
        empleados_df.show()
        hoteles_df.show()

        # Save DataFrames to S3
        empleados_df.write.mode('overwrite').parquet("s3a://hoteles/empleados/")
        hoteles_df.write.mode('overwrite').parquet("s3a://hoteles/hoteles/")

    except Exception as e:
        print("Error reading data from PostgreSQL:", e)

    finally:
        # Stop SparkSession
        spark.stop()


if __name__ == "__main__":
    read_from_postgres()
