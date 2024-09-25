from sqlalchemy import create_engine
import pymysql
import pandas as pd
import boto3
from botocore.exceptions import NoCredentialsError ,ClientError
from botocore.client import Config
from io import BytesIO
import json

def read_s3(bucket_name,file):
    """Funcion que lee un archivo tipo parquet en el bucket s3 (Data Lake)"""

    # Configurar tiempo de espera
    config = Config(connect_timeout=600, read_timeout=600)
    s3_client = boto3.client('s3', config=config)

    try:
        # Cargar el archivo parquet
        response = s3_client.get_object(Bucket=bucket_name, Key=file)
        content = BytesIO(response['Body'].read()) # contenido del archivo tipo parquet
        
        # Leer el archivo Parquet 
        df = pd.read_parquet(content,engine='pyarrow')
        

        return df
        
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            print("El archivo no fue encontrado")
        else:
            print(f"Error al cargar el archivo: {e}")
        
    except NoCredentialsError:
        
        print("Credenciales no disponibles")
        
    except Exception as e:
        
        print(f"Error al cargar el archivo: {e}")
        


bucket_name = 'google-wh' # Nombre del bucket
file = 'sitios/sitios-test-WH.parquet' # Ruta del archivo parquet

df = read_s3(bucket_name,file) # obtenemos el archivo parquet desde el s3
    


def connection_rds():
    """Funcion que realiza la conexiobn a base de datos"""
    
        
    # Datos de conexión a la RDS de Amazon
    host = 'data-base-mysql.ctksme0qs9yz.us-west-1.rds.amazonaws.com' 
    port = 3306
    user = 'paydelimon'  
    password = 'limon123' 
    database = 'Google'  
    
    
    try:
        
        engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}')
        print("Conexión exitosa a RDS")
        
        return engine
        
       
        
    except pymysql.MySQLError as e:
        
        print(f"Error al conectar a la base de datos: {e}")
         
        

def insert_df_rds(df,engine):
       """Funcion que inserta el dataframe en las tablas de Google"""
   
       try:
           
           df.to_sql('sites',con=engine,if_exists='replace',index=False)
           print("Insercion exitosa a la base de datos RDS MySQL")
           
       except pymysql.MySQLError as e:
            
            print(f"Error al conectar a la base de datos: {e}")
        
       except pymysql.IntegrityError as e:
            print(f"Error de integridad: {e}")
        
       except pymysql.DataError as e:
            print(f"Error de datos: {e}")




engine = connection_rds()#obtenemos una conexion

insert_df_rds(df,engine) # insertamos en rds

engine.dispose() # Cerramos la conexion 
