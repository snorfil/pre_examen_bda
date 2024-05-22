from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SparkDataFrameOperations") \
    .getOrCreate()

# Cargar Datos

# Leer CSV
df_csv = spark.read.option("header", True).csv("path/to/csvfile.csv")

# Leer JSON
df_json = spark.read.option("multiline", "true").json("path/to/jsonfile.json")

# Leer Parquet
df_parquet = spark.read.parquet("path/to/parquetfile.parquet")

# Leer desde JDBC (por ejemplo, PostgreSQL)
jdbc_url = "jdbc:postgresql://database:5432/dbname"
connection_properties = {"user": "username", "password": "password", "driver": "org.postgresql.Driver"}
df_jdbc = spark.read.jdbc(url=jdbc_url, table="tablename", properties=connection_properties)

### Mostrar y Examinar Datos


# Mostrar las primeras filas
df.show()

# Mostrar el esquema del DataFrame
df.printSchema()

# Mostrar el resumen estadístico
df.describe().show()


#Seleccionar y Filtrar


# Seleccionar columnas específicas
df.select("column1", "column2").show()

# Filtrar filas
df.filter(df["column1"] > 50).show()

# Filtrar con múltiples condiciones
df.filter((df["column1"] > 50) & (df["column2"] == "value")).show()

##  Ordenar y Agruparç


# Ordenar por una columna
df.orderBy("column1").show()

# Ordenar por múltiples columnas
df.orderBy(df["column1"].desc(), df["column2"].asc()).show()

# Agrupar por una columna y contar
df.groupBy("column1").count().show()

# Agrupar y realizar agregaciones
from pyspark.sql.functions import avg, sum

df.groupBy("column1").agg(
    avg("column2").alias("average"),
    sum("column3").alias("total")
).show()

# Unir DataFrames

# Inner join
df_joined = df1.join(df2, df1["common_column"] == df2["common_column"], "inner")
df_joined.show()

# Left join
df_left_joined = df1.join(df2, df1["common_column"] == df2["common_column"], "left")
df_left_joined.show()

# Right join
df_right_joined = df1.join(df2, df1["common_column"] == df2["common_column"], "right")
df_right_joined.show()

# Full outer join
df_full_outer = df1.join(df2, df1["common_column"] == df2["common_column"], "outer")
df_full_outer.show()

# Manipulación de Columnas

# Añadir una nueva columna
from pyspark.sql.functions import lit

df = df.withColumn("new_column", lit(0))
df.show()

# Renombrar una columna
df = df.withColumnRenamed("old_column", "new_column")
df.show()

# Borrar una columna
df = df.drop("column_to_drop")
df.show()


# Funciones Avanzadas

# Explode (desanidar una columna de tipo array o map)
from pyspark.sql.functions import explode

df_exploded = df.withColumn("exploded_column", explode("array_column"))
df_exploded.show()

# UDF (User Defined Function)
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def custom_function(value):
    return value.upper()

udf_custom_function = udf(custom_function, StringType())
df = df.withColumn("new_column", udf_custom_function(df["existing_column"]))
df.show()


## Escritura de Datos


# Escribir como CSV
df.write.csv("path/to/output/csv", header=True, mode="overwrite")

# Escribir como JSON
df.write.json("path/to/output/json", mode="overwrite")

# Escribir como Parquet
df.write.parquet("path/to/output/parquet", mode="overwrite")

# Escribir a JDBC
df.write.jdbc(url=jdbc_url, table="new_table_name", mode="overwrite", properties=connection_properties)



## Acciones y Persistencia

# Acción: Contar filas
row_count = df.count()
print(row_count)

# Acción: Mostrar las primeras filas como una lista de filas
rows = df.head(10)
for row in rows:
    print(row)

# Persistir un DataFrame en memoria
df.persist()
df.show()

# Liberar DataFrame de la memoria
df.unpersist()
