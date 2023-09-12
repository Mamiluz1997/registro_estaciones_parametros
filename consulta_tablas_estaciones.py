import os
import time
import pandas as pd
import psycopg2
from coneccion_postgres import coneccion_postgres

# Función para ejecutar una consulta SQL y devolver un DataFrame de pandas
def execute_sql_query(query, connection, params=None):
    try:
        # Establecer la conexión a la base de datos
        conn = psycopg2.connect(connection)
        
        # Ejecutar la consulta SQL y cargar los resultados en un DataFrame
        df = pd.read_sql_query(query, conn, params=params)
        
        # Cerrar la conexión a la base de datos
        conn.close()
        
        return df
    except Exception as e:
        print(f"Error al ejecutar la consulta SQL: {str(e)}")
        return None

# Crear una carpeta llamada "estaciones_inicio" si no existe
if not os.path.exists("estaciones_inicio"):
    os.mkdir("estaciones_inicio")

# Leer los esta__id desde un archivo CSV
df_esta_id = pd.read_csv('estaciones.csv')

# Obtener la cadena de conexión
connection_string = coneccion_postgres()

# Crear una lista de esta__id desde el DataFrame leído
esta_id_list = df_esta_id['esta__id'].tolist()

# Medir el tiempo de ejecución
start_time = time.time()

# Nombre de la tabla a consultar
table_name = 'data_m01'

# Consultar la tabla especificada
query = f"""
    SELECT DISTINCT esta__id
    FROM "storage".{table_name}
"""
df_table_ids = execute_sql_query(query, connection_string)

if df_table_ids is not None:
    # Filtrar los esta__id que existen tanto en el CSV como en la base de datos
    common_ids = set(esta_id_list).intersection(set(df_table_ids['esta__id']))
    
    for esta__id in common_ids:
        query = f"""
            SELECT esta__id, copa__id, datafetd
            FROM "storage".{table_name}
            WHERE esta__id = %s
        """
        df_table = execute_sql_query(query, connection_string, [esta__id])
        
        if df_table is not None:
            # Generar el nombre del archivo CSV
            file_name = f"estaciones_inicio/{esta__id}_{table_name}.csv"
            
            # Guardar el DataFrame en el archivo CSV
            df_table.to_csv(file_name, index=False)
            print(f"Archivo CSV {file_name} creado para esta__id {esta__id} en la tabla {table_name}.")

# Calcular y mostrar el tiempo de ejecución
end_time = time.time()
execution_time = end_time - start_time
print(f"Proceso completado en {execution_time:.2f} segundos.")
