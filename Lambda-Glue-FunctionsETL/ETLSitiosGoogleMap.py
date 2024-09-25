import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import mysql.connector
import os

# retorna un numero entero que es la distancia en millas entre dos coordenadas
def haversine(lon1, lat1, lon2, lat2):
    """
    Calcula la distancia entre dos puntos en la Tierra utilizando la fórmula de Haversine.
    La distancia se devuelve en millas.
    """
    R = 3958.8  # Radio de la Tierra en millas
    lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = np.sin(dlat/2.0)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2.0)**2
    c = 2 * np.arcsin(np.sqrt(a))
    return R * c

# Retorna un Dataframe filtrado
def filtrar_cercanos(df1, df2, distancia_max):
    """
    Filtra los registros en df1 que están cerca de cualquier registro en df2.
    Asocia cada registro en df1 con el estadio más cercano en df2.
    """
    resultados = []

    # Iterar sobre cada punto en df1
    for index1, row1 in df1.iterrows():
        lat1, lon1  = row1['latitude'], row1['longitude']
        estadio_mas_cercano = None
        distancia_minima = distancia_max

        # Calcular la distancia a cada punto en df2
        for index2, row2 in df2.iterrows():
            lat2, lon2 = row2['latitude'], row2['longitude']
            distancia = haversine(lon1, lat1, lon2, lat2)
            
            # Si la distancia está dentro del límite y es la mínima, actualizar el estadio más cercano
            if distancia <= distancia_minima:
                distancia_minima = distancia
                estadio_mas_cercano = row2['stadium']
        
        # Si encontramos un estadio cercano, añadir el registro al resultado
        if estadio_mas_cercano:
            registro = row1.copy()
            registro['stadium_cercano'] = estadio_mas_cercano
            registro['distancia_millas'] = distancia_minima
            resultados.append(registro)
    
    # Retornar un DataFrame con los registros cercanos y el estadio más cercano
    return pd.DataFrame(resultados)

#Crea una conexion con la base de datos en rds
def connection_rds():
    try:
        # Crear el engine usando SQLAlchemy
        engine = create_engine(
            'mysql+mysqlconnector://paydelimon:limon123@mysql-db.cpmoyou2oi10.us-west-1.rds.amazonaws.com:3306/Google'
        )
        return engine
    
    except Exception as e:
        print(f"Error al conectar a la base de datos: {e}")
        return None

# retorna un dataframe Creado para los estadios del mundial
def cargar_estadios():
    """
    Genera un Dataframe Unicamente con los datos que corresponden a los estadios sedes del mundial
    """
    data = {
        'stadium': ['Mercedes-Benz Stadium', 'Gillette Stadium', 'AT&T Stadium', 'NRG Stadium', 'Arrowhead Stadium', 
                    'SoFi Stadium', 'Hard Rock Stadium', 'MetLife Stadium', 'Lincoln Financial Field', 
                    "Levi's Stadium", 'Lumen Field'],
        'city': ['Atlanta', 'Boston', 'Dallas', 'Houston', 'Kansas City', 'Los Angeles', 'Miami', 
                 'New York/New Jersey', 'Philadelphia', 'San Francisco Bay Area', 'Seattle'],
        'latitude': [33.755409, 42.090970, 32.748047, 29.684750, 39.049031, 33.953510, 25.958384, 
                     40.813555, 39.901476, 37.403393, 47.595188],
        'longitude': [-84.400560, -71.264336, -97.093394, -95.410697, -94.484025, -118.339050, 
                      -80.239587, -74.074457, -75.167200, -121.969388, -122.331639]
    }

    # Crear el DataFrame
    stadiums_df = pd.DataFrame(data)
    
    return stadiums_df

# Retorna un DataFrame con los datos en crudo según el tipo de archivo
def cargar_datos_sitios(ruta_base):
    # Verificar la extensión del archivo y cargar de acuerdo al formato
    try:    
        if ruta_base.endswith('.json'):
            return pd.read_json(ruta_base, lines=True)
        elif ruta_base.endswith('.csv'):
            return pd.read_csv(ruta_base)
        elif ruta_base.endswith('.parquet'):
            return pd.read_parquet(ruta_base)
        elif ruta_base.endswith('.xlsx') or ruta_base.endswith('.xls'):
            return pd.read_excel(ruta_base)
        elif ruta_base.endswith('.xml'):
            return pd.read_xml(ruta_base)
        else:
            raise ValueError(f"Formato de archivo no soportado: {ruta_base}")
    except Exception as e:
            print(f"Error al cargar  los datos: {e}")    


#Filtra los datos y limpia la informacion 
def transformar_datos_sitios(ruta_base , sedes):
    
    try:
        df = cargar_datos_sitios(ruta_base)
        df.drop_duplicates(keep='last' , inplace=True , subset=['gmap_id'])
        cercanos = filtrar_cercanos(df, sedes , 20)
        cercanos['category'] = cercanos['category'].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)
        cercanos_filtrados = cercanos.drop(['price','description','hours','MISC','state','relative_results'] , axis=1)
        return cercanos_filtrados
    except Exception as e:
        print(f"Error en la transformacion de los datos sitios : {e}")
        return None    

def guardar_dataframe(df):
    conn = connection_rds()
    
    if conn is not None:
        try:
            # Guardar el DataFrame en la base de datos
            df.to_sql('sites', conn, if_exists='append', index=False)
            print("Datos guardados correctamente.")
        except Exception as e:
            print(f"Error al guardar los datos: {e}")
        finally:
            conn.dispose()  # Cerrar la conexión del engine
    else:
        print("No se pudo establecer la conexión.")

#                  ___                       ___                       ___     
#       ___        /\__\          ___        /\  \          ___        /\  \    
#      /\  \      /::|  |        /\  \      /::\  \        /\  \      /::\  \   
#      \:\  \    /:|:|  |        \:\  \    /:/\:\  \       \:\  \    /:/\:\  \  
#      /::\__\  /:/|:|  |__      /::\__\  /:/  \:\  \      /::\__\  /:/  \:\  \ 
#   __/:/\/__/ /:/ |:| /\__\  __/:/\/__/ /:/__/ \:\__\  __/:/\/__/ /:/__/ \:\__\
#  /\/:/  /    \/__|:|/:/  / /\/:/  /    \:\  \  \/__/ /\/:/  /    \:\  \ /:/  /
#  \::/__/         |:/:/  /  \::/__/      \:\  \       \::/__/      \:\  /:/  / 
#   \:\__\         |::/  /    \:\__\       \:\  \       \:\__\       \:\/:/  /  
#    \/__/         /:/  /      \/__/        \:\__\       \/__/        \::/  /   
#                  \/__/                     \/__/                     \/__/   





df_estadios = cargar_estadios()


url_base = 'Lambda\\sitios'  # Asegúrate de usar doble barra invertida o una barra normal

# Listar todos los archivos en el directorio
# Listar todos los archivos en el directorio
try:
    archivos = os.listdir(url_base)
    for archivo in archivos:
        path_completo = os.path.join(url_base, archivo)
        print(path_completo)
        df = cargar_datos_sitios(path_completo)
        print('Dataframe cargado.....', df.shape)  # Acceso correcto al atributo
        df.drop_duplicates(keep='last', inplace=True, subset=['gmap_id'])
        print('Dataframe sin duplicados.....', df.shape)
        cercanos = filtrar_cercanos(df, df_estadios, 20)
        print('Dataframe Filtrado', cercanos.shape)
        cercanos['category'] = cercanos['category'].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)
        print('Categoras aplanadas')
        cercanos.drop(['price','description','hours','MISC','state','relative_results'] , axis=1, inplace=True)
        print('df filtrado y limpiado')
        guardar_dataframe(cercanos)
        
except FileNotFoundError:
    print(f"El directorio '{url_base}' no se encontró.")
except Exception as e:
    print(f"Ocurrió un error: {e}")


