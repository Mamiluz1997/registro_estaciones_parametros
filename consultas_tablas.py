import pandas as pd
import psycopg2
from coneccion_postgres import coneccion_postgres  # Importa la función de conexión desde tu archivo coneccion_postgres.py

# Función para ejecutar una consulta SQL y devolver un DataFrame de pandas
def execute_sql_query(query, connection):
    try:
        # Establecer la conexión a la base de datos
        conn = psycopg2.connect(connection)
        
        # Ejecutar la consulta SQL y cargar los resultados en un DataFrame
        df = pd.read_sql_query(query, conn)
        
        # Cerrar la conexión a la base de datos
        conn.close()
        
        return df
    except Exception as e:
        print(f"Error al ejecutar la consulta SQL: {str(e)}")
        return None

# Consulta SQL para la tabla data_m01
query_data_m01 = """
    SELECT esta__id, copa__id, datafetd
    FROM "storage".data_m01
    WHERE esta__id = 1
"""

# Consulta SQL para la tabla data_m02
query_data_m02 = """
    SELECT esta__id, copa__id, datafetd
    FROM "storage".data_m02
    WHERE esta__id = 1
"""

# Obtener la cadena de conexión
connection_string = coneccion_postgres()

# Ejecutar las consultas y obtener los DataFrames
df_data_m01 = execute_sql_query(query_data_m01, connection_string)
df_data_m02 = execute_sql_query(query_data_m02, connection_string)

if df_data_m01 is not None and df_data_m02 is not None:
    # Combinar los resultados de ambas tablas en un solo DataFrame
    df_combined = pd.concat([df_data_m01, df_data_m02])
    
    # Guardar el DataFrame combinado en un archivo CSV
    df_combined.to_csv('result_combinacion_tablas.csv', index=False)
    print("Datos guardados en resultado.csv")
else:
    print("No se pudieron obtener los datos de la base de datos.")
