import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame  # Importar DynamicFrame
from pyspark.sql import functions as F  # Importar funciones de PySpark

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Leer los datos de S3
df_users = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="parquet",
    connection_options={"paths": ["s3://yelp-lh/user.parquet"]},
    transformation_ctx="ResolveSource"
).toDF()

# Crear DynamicFrames a partir de las tablas del Data Catalog
users_ids_df = glueContext.create_dynamic_frame.from_catalog(
    database="data_catalog_yelp",
    table_name="users_id"
).toDF()

# Obtener lista de user_id
user_id_list = set(users_ids_df.select("user_id").rdd.flatMap(lambda x: x).collect())

# Filtrar el DataFrame basado en ambas listas y eliminar registros nulos
df_users_cleaned = df_users.filter(
    (F.col("user_id").isin(user_id_list)) &
    (F.col("user_id").isNotNull())
)

# Eliminar registros duplicados
df_users_cleaned.dropDuplicates()

#Eliminar columnas irrelevantes
cols_drop = ['yelping_since', 'elite', 'friends', 'fans', 'compliment_hot', 'compliment_more', 'compliment_profile', 'compliment_cute', 'compliment_list', 'compliment_note', 'compliment_plain', 'compliment_cool', 'compliment_funny', 'compliment_writer', 'compliment_photos']
df_users_cleaned = df_users_cleaned(*cols_drop)

# Convertir el DataFrame filtrado de vuelta a DynamicFrame
df_users_clean = DynamicFrame.fromDF(df_users_cleaned, glueContext, "df_users_clean")

# Escribir los resultados en S3 en formato Parquet
glueContext.write_dynamic_frame.from_options(
    frame=df_users_clean,
    connection_type="s3",
    format="parquet",
    connection_options={"path": "s3://yelp-wh/user.parquet"},
    transformation_ctx="s3output"
)

job.commit()
