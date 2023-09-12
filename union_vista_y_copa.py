import pandas as pd
import os
import time
import psycopg2
from coneccion_postgres import coneccion_postgres

# Obtiene la cadena de conexión a PostgreSQL desde tu función
db_config = coneccion_postgres()

input_folder = 'estaciones_inicio'
output_folder = 'calculo_huecos'
os.makedirs(output_folder, exist_ok=True)

# Conectar a la base de datos PostgreSQL
conn = psycopg2.connect(db_config)

vta_estaciones_query = "SELECT esta__id, puobcodi FROM administrativo.vta_estaciones_todos"
copa_query = "SELECT copa__id, copanemo FROM administrativo.copa"

# Cargar los datos de las consultas a la base de datos
df_estaciones = pd.read_sql_query(vta_estaciones_query, conn)
df_copa = pd.read_sql_query(copa_query, conn)

# Lista todos los archivos CSV en la carpeta de entrada
archivos_csv = [archivo for archivo in os.listdir(input_folder) if archivo.endswith('.csv')]

# Diccionario para almacenar los totales de fechas faltantes por estación
total_fechas_faltantes_por_estacion = {}

# Itera a través de los archivos CSV en la carpeta de entrada
for archivo_csv in archivos_csv:
    # Lee el archivo CSV de la carpeta de entrada
    ruta_archivo = os.path.join(input_folder, archivo_csv)
    df = pd.read_csv(ruta_archivo)

    # Convierte la columna 'datafetd' en un objeto datetime
    df['datafetd'] = pd.to_datetime(df['datafetd'])
    
    # Nombre de la estación a partir del nombre del archivo CSV
    nombre_estacion = archivo_csv.split('_')[0]

    # Agrega las columnas 'codigo' y 'nemonico' a partir de las consultas a la base de datos
    df = df.merge(df_estaciones, left_on='esta__id', right_on='esta__id', how='left')
    df = df.merge(df_copa, left_on='copa__id', right_on='copa__id', how='left')
    
    # Itera a través de los años únicos en el DataFrame
    años_unicos = df['datafetd'].dt.year.unique()
    for año in años_unicos:
        # Filtra el DataFrame de la estación por año
        df_año = df[df['datafetd'].dt.year == año]
        
        # Calcula las fechas faltantes por minuto para el año
        minuto = pd.Timedelta(minutes=1)
        fecha_inicial = df_año['datafetd'].min()
        fecha_final = df_año['datafetd'].max()
        fechas_faltantes = []
        while fecha_inicial <= fecha_final:
            if fecha_inicial not in df_año['datafetd'].values:
                fechas_faltantes.append(fecha_inicial)
            fecha_inicial += minuto
        
        # Crea un DataFrame con las fechas faltantes
        df_fechas_faltantes = pd.DataFrame({'fechas_faltantes': fechas_faltantes})
        
        # Agrega las columnas 'id_estacion', 'codigo', 'id_combinacion_parametro', 'nemonico', 'fechas_faltantes'
        df_fechas_faltantes['id_estacion'] = nombre_estacion
        df_fechas_faltantes['codigo'] = df_año['puobcodi'].iloc[0]  # Tomamos el valor del primer registro
        df_fechas_faltantes['id_combinacion_parametro'] = df_año['copa__id'].iloc[0]  # Tomamos el valor del primer registro
        df_fechas_faltantes['nemonico'] = df_año['copanemo'].iloc[0]  # Tomamos el valor del primer registro
        
        # Reordena las columnas para tener 'id_estacion', 'codigo', 'id_combinacion_parametro', 'nemonico', 'fechas_faltantes'
        df_fechas_faltantes = df_fechas_faltantes[['id_estacion', 'codigo', 'id_combinacion_parametro', 'nemonico', 'fechas_faltantes']]
        
        # Genera un sufijo para el archivo de salida para evitar sobreescribir
        sufijo_archivo_salida = f'{nombre_estacion}_{año}'
        
        # Guarda el DataFrame de fechas faltantes en un archivo CSV en la carpeta de salida
        nombre_archivo = f'{sufijo_archivo_salida}_fechas_faltantes_{año}.csv'
        ruta_archivo_salida = os.path.join(output_folder, nombre_archivo)
        df_fechas_faltantes.to_csv(ruta_archivo_salida, index=False)
        
        # Calcula el total de fechas faltantes por estación
        total_fechas_faltantes = len(fechas_faltantes)
        if nombre_estacion in total_fechas_faltantes_por_estacion:
            total_fechas_faltantes_por_estacion[nombre_estacion] += total_fechas_faltantes
        else:
            total_fechas_faltantes_por_estacion[nombre_estacion] = total_fechas_faltantes

# Calcula el total de todas las fechas faltantes
total_fechas_faltantes_total = sum(total_fechas_faltantes_por_estacion.values())

# Guarda los totales en un archivo de texto
with open('totales_fechas_faltantes.txt', 'w') as txt_file:
    txt_file.write("Totales de fechas faltantes por estación:\n")
    for estacion, total in total_fechas_faltantes_por_estacion.items():
        txt_file.write(f"{estacion}: {total}\n")

    txt_file.write(f"\nTotal de todas las fechas faltantes: {total_fechas_faltantes_total}")

print("Proceso completado.")
