import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import ast
import pandas as pd
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Parametros de entrada y salida
raw_bucket = "s3://yelp-lh/business.pkl"
stage_bucket = "s3://yelp-wh/business-parquet"

# Leer el archivo PKL utilizando pandas
pkl_file_path = "s3://yelp-lh/business.pkl"
df_business = pd.read_pickle(pkl_file_path)

# Transformaciones

# Descartamos las columnas duplicadas del dataframe y solo trabajaremos con las primeras 14
df_business = df_business.iloc[:, :14]

# Completar campos vacios en state
df_business.loc[0,'state'] = 'CA'
df_business.loc[0,'state'] = 'MO'
df_business.loc[0,'state'] = 'AZ'

# Filtrar los estados que conforman el alcance del proyecto
filtered_df = df_business[df_business['state'].isin(['GA','MA','TX','MO','CA','FL','NY','NJ','PA','WA'])].reset_index(drop=True)

# Datos anidados
# Reemplazar NaN o valores no diccionario con diccionario vacío
def validate_dict(data):
    # Si el atributo es un diccionario, lo retorna tal cual
    if isinstance(data, dict):
        return data
    # Si no es un diccionario o es nulo, retorna un diccionario vacío
    return {}
filtered_df.loc[:,'attributes'] = filtered_df['attributes'].apply(validate_dict)

# Convertir la columna attributes en un dataframe
attributes_df = pd.json_normalize(filtered_df['attributes'])

# Unir los atributos al dataframe original eliminando la columna 'attributes'
filtered_df = filtered_df.drop(columns=['attributes']).join(attributes_df)

# Tipos de datos
filtered_df['hours'] = filtered_df['hours'].astype(str)

# Campos vacios
filtered_df.fillna('None', inplace=True)

# Eliminar business_id duplicados
filtered_df = filtered_df.drop_duplicates(subset=['business_id'], keep='last')

# Eliminar columnas irrelevantes
cols_drop = ['RestaurantsGoodForGroups', 'RestaurantsReservations', 'WiFi', 'OutdoorSeating', 'Alcohol', 'is_open', 'Caters', 'WheelchairAccessible', 'RestaurantsTableService', 'HasTV', 'CoatCheck', 'DriveThru', 'HappyHour', 'GoodForMeal', 'BusinessAcceptsBitcoin', 'Smoking', 'Music', 'GoodForDancing', 'BestNights', 'BYOB', 'Corkage', 'AcceptsInsurance', 'BYOBCorkage', 'HairSpecializesIn', 'RestaurantsCounterService', 'AgesAllowed', 'Open24Hours', 'DietaryRestrictions']
filtered_df = filtered_df.drop(columns=cols_drop)

# Limpieza (listas y caracteres no deseados)
def filter_true_keys(ambience):
    if isinstance(ambience, dict):
        # Filtrar y conservar solo las claves con valor True
        return [key for key, value in ambience.items() if value]

def clean_dict(s):
    if isinstance(s, str):
        # Eliminar el prefijo 'u' en cadenas, y también las comillas simples y dobles si están presentes
        s = s.replace("u'", "'").replace("u\"", "\"")
    try:
        # Evaluar la cadena para convertirla en un diccionario
        dict_obj = ast.literal_eval(s)
        if isinstance(dict_obj, dict):
            # Limpiar las claves si es un diccionario
            return {key[2:-1] if key.startswith('u\'') and key.endswith('\'') else key: value for key, value in dict_obj.items()}
        else:
            return dict_obj
    except (ValueError, SyntaxError):
        return s

# Aplicar las funciones a las columnas con datos anidados y que conserven solo el valor True del diccionario
filtered_df['Ambience'] = filtered_df['Ambience'].apply(clean_dict)
filtered_df['RestaurantsAttire'] = filtered_df['RestaurantsAttire'].apply(clean_dict)
filtered_df['NoiseLevel'] = filtered_df['NoiseLevel'].apply(clean_dict)
filtered_df['BusinessParking'] = filtered_df['BusinessParking'].apply(clean_dict)

filtered_df['Ambience'] = filtered_df['Ambience'].apply(filter_true_keys)
filtered_df['BusinessParking'] = filtered_df['BusinessParking'].apply(filter_true_keys)

# Función para agregar RestaurantsAttire a Ambience si no está presente
def add_attire_to_ambience(row):
    if isinstance(row['Ambience'], list) and isinstance(row['RestaurantsAttire'], str):
        if row['RestaurantsAttire'] not in row['Ambience']:
            row['Ambience'].append(row['RestaurantsAttire'])
    return row

filtered_df = filtered_df.apply(add_attire_to_ambience, axis=1)

# Eliminar la columna RestaurantsAttire
filtered_df = filtered_df.drop(columns=['RestaurantsAttire'])

# Convertir a un DataFrame de Spark
df_clean = spark.createDataFrame(filtered_df)

# Convertir el DF a un DynamicFrame
dynamicFrame_clean = DynamicFrame.fromDF(df_clean, glueContext, "dynamicFrame_clean")

# Guardar los business_id en Data Catalog

# Conexion con Glue Data Catalog
database_name = "data_catalog_yelp"
table_name = "business_id_yelp"

# Filtrar solo la columna business_id
dynamicFrame_filtered = SelectFields.apply(frame=dynamicFrame_clean, paths=["business_id"])

# Escribir el DynamicFrame en Glue Data Catalog
glueContext.write_dynamic_frame.from_catalog(
    frame = dynamicFrame_filtered,
    database = database_name,
    table_name = table_name,
    transformation_ctx = "datasink"
)

# Guardar los datos limpios con formato parquet en el bucket stage
s3output = glueContext.getSink(
  path=stage_bucket,
  connection_type="s3",
  partitionKeys=[],
  compression="snappy",
  transformation_ctx="s3output",
)
s3output.setFormat("glueparquet")
s3output.writeFrame(dynamicFrame_clean)
