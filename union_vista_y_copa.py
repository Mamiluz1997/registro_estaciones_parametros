import pandas as pd
import os
import psycopg2
from coneccion_postgres import coneccion_postgres  # Importa tu función de conexión

# Obtiene la cadena de conexión a PostgreSQL desde tu función
db_config = coneccion_postgres()

# Crear una carpeta para almacenar los archivos CSV
output_folder = 'fechas_faltantes_por_año'
os.makedirs(output_folder, exist_ok=True)

# Cargar el archivo CSV generado anteriormente
df_combined = pd.read_csv('result_huecos.csv')

# Convertir la columna 'fechas_faltantes' a tipo de dato datetime
df_combined['fechas_faltantes'] = pd.to_datetime(df_combined['fechas_faltantes'])

# Obtener una lista de años únicos en los datos
unique_years = df_combined['fechas_faltantes'].dt.year.unique()

# Inicializar la conexión a la base de datos PostgreSQL
conn = psycopg2.connect(db_config)

# Crear un cursor para ejecutar consultas SQL
cursor = conn.cursor()

# Consulta SQL para obtener información de la vista vta_estaciones_todos
vta_estaciones_query = "SELECT esta__id, puobcodi FROM administrativo.vta_estaciones_todos"

# Consulta SQL para obtener información de la tabla copa en el esquema administrador
copa_query = "SELECT copa__id, copanemo FROM administrativo.copa"

# Iterar sobre cada año y realizar la comparación
for year in unique_years:
    # Filtrar los datos para el año actual
    df_year = df_combined[df_combined['fechas_faltantes'].dt.year == year]
    
    # Realizar la comparación con la vista vta_estaciones_todos
    df_year = df_year.merge(pd.read_sql(vta_estaciones_query, conn), left_on='id_estacion', right_on='esta__id', how='left')
    
    # Realizar la comparación con la tabla copa en el esquema administrador
    df_year = df_year.merge(pd.read_sql(copa_query, conn), left_on='id_combinacion_parametro', right_on='copa__id', how='left')
    
    # Renombrar las columnas
    df_year.rename(columns={'puobcodi': 'codigo', 'copanemo': 'nemonico'}, inplace=True)
    
    # Generar el nombre del archivo CSV para el año
    file_name = os.path.join(output_folder, f'{df_year["codigo"].iloc[0]}_fechas_faltantes_{year}.csv')
    
    # Guardar los datos en el archivo CSV correspondiente al año
    df_year.to_csv(file_name, index=False, columns=['id_estacion', 'id_combinacion_parametro', 'codigo', 'nemonico', 'fechas_faltantes'])
    
    print(f'Datos del año {year} guardados en {file_name}')

# Cerrar la conexión a la base de datos
conn.close()
