import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, row_number  # Para operaciones de columnas y numeración
from pyspark.sql.window import Window  # Para operaciones de ventana
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F  # Importa las funciones de SQL
from pyspark.sql.types import ArrayType, StringType
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Leer los datos de S3
df_tips = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="parquet",
    connection_options={"paths": ["s3://yelp-lh/tip.parquet"]},
    transformation_ctx="ResolveSource"
).toDF()

# Crear DynamicFrames a partir de las tablas del Data Catalog
business_ids_df = glueContext.create_dynamic_frame.from_catalog(
    database="data_catalog_yelp",
    table_name="business_id_yelp"
).toDF()

users_ids_df = glueContext.create_dynamic_frame.from_catalog(
    database="data_catalog_yelp",
    table_name="users_id"
).toDF()
business_ids_df.printSchema()
users_ids_df.printSchema()

# Obtener listas de business_id y user_id
business_id_list = set(business_ids_df.select("business_id").rdd.flatMap(lambda x: x).collect())
user_id_list = set(users_ids_df.select("user_id").rdd.flatMap(lambda x: x).collect())

# Filtrar el DataFrame basado en ambas listas y eliminar registros nulos
df_cleaned = df_tips.filter(
    (F.col("business_id").isin(business_id_list)) &
    (F.col("user_id").isin(user_id_list)) &
    (F.col("business_id").isNotNull()) &
    (F.col("user_id").isNotNull())
)

# Eliminar registros duplicados basados en business_id y user_id
windowSpec = Window.partitionBy("business_id", "user_id").orderBy("date")
df_cleaned = df_cleaned.withColumn("row_number", F.row_number().over(windowSpec)) \
                       .filter(F.col("row_number") == 1) \
                       .drop("row_number")

# Convertir el DataFrame filtrado de vuelta a DynamicFrame
df_tip = DynamicFrame.fromDF(df_cleaned, glueContext, "df_tip")

# Escribir los resultados en S3 en formato Parquet
glueContext.write_dynamic_frame.from_options(
    frame=df_tip,
    connection_type="s3",
    format="parquet",
    connection_options={"path": "s3://yelp-wh/tip.parquet"},
    transformation_ctx="s3output"
)
