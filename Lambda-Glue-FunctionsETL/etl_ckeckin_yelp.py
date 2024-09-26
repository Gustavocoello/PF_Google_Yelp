import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame  # Importar DynamicFrame
from pyspark.sql import functions as F  # Importar funciones de PySpark
from pyspark.sql.functions import col

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Leer los datos de S3 y convertir el DynamicFrame a DataFrame para poder utilizar funciones de SQL
df_checkin = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="parquet",
    connection_options={"paths": ["s3://yelp-lh2/checkin.parquet"]},
    transformation_ctx="ResolveSource"
).toDF()

# Crear un DynamicFrame a partir de la tabla del Data Catalog
business_ids_df = glueContext.create_dynamic_frame.from_catalog(
    database="data_catalog_yelp",
    table_name="business_id_yelp"
).toDF()

# Obtener una lista de los business_id de la tabla
business_id_list = set(business_ids_df.select("business_id").rdd.flatMap(lambda x: x).collect())

# Filtrar el DataFrame basado en la lista y eliminar registros nulos
df_checkin_cleaned = df_checkin.filter(
    (F.col("business_id").isin(business_id_list)) &
    (F.col("business_id").isNotNull()) 
)

# Dividir la columna 'date'
df_checkin_cleaned = df_checkin_cleaned.withColumn("date_checkin", F.explode(F.split(col("date"), ",")))
df_checkin_cleaned = df_checkin_cleaned.drop("date")

# Eliminar duplicados
df_checkin_cleaned = df_checkin_cleaned.dropDuplicates()

# Convertir el DataFrame filtrado a DynamicFrame
df_checkin_clean = DynamicFrame.fromDF(df_checkin_cleaned, glueContext, "df_checkin_clean")

# Escribir los resultados en S3 en formato Parquet
glueContext.write_dynamic_frame.from_options(
    frame=df_checkin_clean,
    connection_type="s3",
    format="parquet",
    connection_options={"path": "s3://yelp-wh/checkin.parquet"},
    transformation_ctx="s3output"
)
job.commit()
