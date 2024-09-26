import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F  # Importa las funciones de SQL
from pyspark.sql.functions import udf, col
from pyspark.sql.types import ArrayType, StringType
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Leer los datos de S3 y convertir en dataframe
df_reviews = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="parquet",
    connection_options={"paths": ["s3://yelp-lh/review-002.parquet/"]},
    transformation_ctx="ResolveSource"
).toDF()

# Crear un DynamicFrame a partir de la tabla del Data Catalog
business_id = glueContext.create_dynamic_frame.from_catalog(database='data_catalog_yelp', table_name='business_id_yelp').toDF()

# Obtener una lista de los business_id de la tabla
business_id_list = business_id.select("business_id").rdd.flatMap(lambda x: x).collect()
df_reviews.printSchema()

# Crear una UDF para verificar si el business_id está en la lista
@udf(ArrayType(StringType()))
def is_in_business_id_list(business_id):
    return [business_id] if business_id in business_id_list else []

# Aplicar la UDF para filtrar los datos
df = df_reviews.filter(is_in_business_id_list(col("business_id")).isNotNull())

# Eliminar registros donde business_id o user_id sean nulos
df = df.filter((col("business_id").isNotNull()) & (col("user_id").isNotNull()))

# Eliminar duplicados basados en 'review_id'
df = df.dropDuplicates(["review_id"])

# Extraer los user_id únicos para la tabla users_id
user_ids_df = df.select("user_id").distinct()

# Convertir el DataFrame a DynamicFrame
dynamic_userID = DynamicFrame.fromDF(user_ids_df, glueContext, "my_dynamic_frame2")

# Escribe el DataFrame en la tabla del Data Catalog
glueContext.write_dynamic_frame.from_catalog(
    frame=dynamic_userID,
    database="data_catalog_yelp",
    table_name="users_id",
    transformation_ctx="WriteToCatalog"
)

# Guardar los datos en S3
dynamic_frame = DynamicFrame.fromDF(df, glueContext, "my_dynamic_frame")
glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame,
    connection_type="s3",
    format="parquet",
    connection_options={"path": "s3://yelp-wh/reviews.parquet"},
    transformation_ctx="s3output"
)
