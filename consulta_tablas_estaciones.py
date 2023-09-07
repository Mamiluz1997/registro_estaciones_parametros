import pandas as pd
import psycopg2
from coneccion_postgres import coneccion_postgres  # Importa la función de conexión desde tu archivo coneccion_postgres.py

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

# Leer los esta__id desde un archivo CSV
df_esta_id = pd.read_csv('estaciones.csv')

# Obtener la cadena de conexión
connection_string = coneccion_postgres()

# Crear una lista de esta__id desde el DataFrame leído
esta_id_list = df_esta_id['esta__id'].tolist()

# Construir la consulta SQL para obtener los datos de las 12 tablas
# Utiliza la lista de esta__id en una cláusula IN en cada consulta
# y combina los resultados con UNION ALL
placeholders = ', '.join(['%s'] * len(esta_id_list))
query_combined = """
    SELECT esta__id, copa__id, datafetd
    FROM "storage".data_m01
    WHERE esta__id IN ({})
    
    UNION ALL
    
    SELECT esta__id, copa__id, datafetd
    FROM "storage".data_m02
    WHERE esta__id IN ({})
    
    -- Repite este patrón para las otras 10 tablas
""".format(placeholders, placeholders)

# Ejecutar la consulta y obtener el DataFrame resultante
df_combined = execute_sql_query(query_combined, connection_string, esta_id_list * 2)

if df_combined is not None:
    # Guardar el DataFrame combinado en un archivo CSV
    df_combined.to_csv('result_combinacion_tablas.csv', index=False)
    print("Datos guardados en resultado.csv")
else:
    print("No se pudieron obtener los datos de la base de datos.")
