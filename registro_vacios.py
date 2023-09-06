import pandas as pd

# Cargar el archivo CSV generado anteriormente
df_combined = pd.read_csv('result_combinacion_tablas.csv')

# Convertir la columna 'datafetd' a tipo de dato datetime
df_combined['datafetd'] = pd.to_datetime(df_combined['datafetd'])

# Obtener la lista de todas las combinaciones únicas de id_estacion y copa_id
combinations = df_combined[['esta__id', 'copa__id']].drop_duplicates()

# Crear un DataFrame vacío para almacenar las fechas faltantes
df_missing_dates = pd.DataFrame(columns=['id_estacion', 'id_combinacion_parametro', 'fechas_faltantes'])

# Variable para almacenar el total de fechas faltantes
total_huecos = 0

# Iterar a través de las combinaciones únicas y encontrar las fechas faltantes para cada una
for _, row in combinations.iterrows():
    esta_id = row['esta__id']
    copa_id = row['copa__id']
    
    # Filtrar el DataFrame original para la combinación actual
    filtered_df = df_combined[(df_combined['esta__id'] == esta_id) & (df_combined['copa__id'] == copa_id)]
    
    # Crear un rango de fechas completo para la combinación actual
    min_date = filtered_df['datafetd'].min()
    max_date = filtered_df['datafetd'].max()
    date_range = pd.date_range(start=min_date, end=max_date, freq='T')
    
    # Encontrar las fechas faltantes restando el rango completo y las fechas existentes
    missing_dates = date_range[~date_range.isin(filtered_df['datafetd'])]
    
    # Crear un DataFrame con las fechas faltantes para esta combinación
    missing_df = pd.DataFrame({'id_estacion': [esta_id] * len(missing_dates),
                               'id_combinacion_parametro': [copa_id] * len(missing_dates),
                               'fechas_faltantes': missing_dates})
    
    # Agregar las fechas faltantes al DataFrame final
    df_missing_dates = pd.concat([df_missing_dates, missing_df], ignore_index=True)
    
    # Actualizar el total de fechas faltantes
    total_huecos += len(missing_dates)

# Guardar las fechas faltantes en un archivo CSV
df_missing_dates.to_csv('result_huecos.csv', index=False)
print("fechas faltantes guardadas")

# Guardar la cantidad total de fechas faltantes en un archivo de texto
with open('total_huecos.txt', 'w') as txt_file:
    txt_file.write(f'Total de Huecos: {total_huecos}')

print(f'Se ha guardado el total de huecos en total_huecos.txt: {total_huecos}')
