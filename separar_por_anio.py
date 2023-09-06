import pandas as pd
import os

# Crear una carpeta para almacenar los archivos CSV
output_folder = 'fechas_faltantes_por_año'
os.makedirs(output_folder, exist_ok=True)

# Cargar el archivo CSV generado anteriormente
df_combined = pd.read_csv('result_huecos.csv')

# Convertir la columna 'fechas_faltantes' a tipo de dato datetime
df_combined['fechas_faltantes'] = pd.to_datetime(df_combined['fechas_faltantes'])

# Obtener una lista de años únicos en los datos
unique_years = df_combined['fechas_faltantes'].dt.year.unique()

# Crear un diccionario para almacenar el total de fechas faltantes por año
total_fechas_faltantes_por_año = {}

# Iterar sobre cada año y contar las fechas faltantes
for year in unique_years:
    # Filtrar los datos para el año actual
    df_year = df_combined[df_combined['fechas_faltantes'].dt.year == year]
    
    # Calcular el total de fechas faltantes para el año actual
    total_fechas_faltantes = len(df_year)
    
    # Almacenar el total en el diccionario
    total_fechas_faltantes_por_año[year] = total_fechas_faltantes

    # Generar el nombre del archivo CSV para el año
    file_name = os.path.join(output_folder, f'fechas_faltantes_{year}.csv')
    
    # Guardar los datos en el archivo CSV correspondiente al año
    df_year.to_csv(file_name, index=False)
    
    print(f'Datos del año {year} guardados en {file_name}. Total de fechas faltantes: {total_fechas_faltantes}')

# Guardar el total de fechas faltantes en un archivo de texto
with open(os.path.join(output_folder, 'total_fechas_faltantes.txt'), 'w') as file:
    file.write("Total de fechas faltantes por año:\n")
    for year, total in total_fechas_faltantes_por_año.items():
        file.write(f'Año {year}: {total}\n')

print(f"Total de fechas faltantes por año guardado en {os.path.join(output_folder, 'total_fechas_faltantes.txt')}")
