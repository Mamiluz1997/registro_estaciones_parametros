from sqlalchemy import create_engine
import pandas as pd
import coneccion_postgres
import os

def main():
    # Obtener la cadena de conexión
    connection_string = coneccion_postgres.coneccion_postgres()

    # Crear el objeto Engine utilizando SQLAlchemy
    engine = create_engine(connection_string)

    # Consulta SQL para obtener los registros, incluyendo el código de la vista "vta_estaciones"
    select_query = """
    SELECT 
        dm.esta__id AS id_estacion,
        ac.copanemo AS nemonico,
        ac.copa__id AS id_combinacion_parametro,
        dm.datafetd AS fecha_faltante,
        vet.puobcodi AS codigo
    FROM 
        "storage".data_m01 dm
    JOIN 
        "administrativo".copa ac ON dm.esta__id::text = ac.copa__id::text
    JOIN
        "administrativo".vta_estaciones_todos vet ON dm.esta__id::text = vet.esta__id::text
    -- Aquí utilizamos la columna "esta__id" de "vta_estaciones_todos" para el join
    ORDER BY 
        id_estacion, dm.datafetd;
    """

    # Utilizar Pandas para cargar los datos desde la base de datos
    df = pd.read_sql_query(select_query, engine)

    # Convertir la columna 'datafetd' a tipo datetime
    df['fecha_faltante'] = pd.to_datetime(df['fecha_faltante'])

    # Obtener la lista de estaciones y combinaciones de parámetros únicos
    unique_stations = df['id_estacion'].unique()
    unique_combinations = df['id_combinacion_parametro'].unique()

    for station in unique_stations:
        for combination in unique_combinations:
            station_combination_df = df[(df['id_estacion'] == station) & (df['id_combinacion_parametro'] == combination)]

            # Verificar si hay datos antes de calcular las fechas faltantes
            if not station_combination_df.empty:
                # Convertir la columna 'fecha_faltante' en un índice para facilitar la comparación
                station_combination_df.set_index('fecha_faltante', inplace=True)

                # Crear un rango de fechas completo para esa estación y combinación con la frecuencia adecuada
                date_range = pd.date_range(start=station_combination_df.index.min(), end=station_combination_df.index.max(), freq='T')

                # Encontrar las fechas faltantes
                missing_dates = date_range[~date_range.isin(station_combination_df.index)]

                # Crear un DataFrame de fechas faltantes con todas las columnas necesarias
                missing_dates_df = pd.DataFrame(data={'fechas_faltantes': missing_dates})
                missing_dates_df['id_estacion'] = station_combination_df['id_estacion'][0]
                missing_dates_df['codigo'] = station_combination_df['codigo'][0]  # Utilizar 'codigo' para el nombre de archivo
                missing_dates_df['nemonico'] = station_combination_df['nemonico'][0]
                missing_dates_df['id_combinacion_parametro'] = station_combination_df['id_combinacion_parametro'][0]

                # Verificar si hay fechas faltantes
                if not missing_dates_df.empty:
                    # Agregar el año al nombre del archivo CSV
                    output_dir = f'estacion_{station}'
                    year = missing_dates_df['fechas_faltantes'].iloc[0].year  # Obtener el año de la primera fecha faltante
                    csv_filename = f'{output_dir}/{station_combination_df["codigo"].iloc[0]}_fechas_faltantes_{year}.csv'

                    # Crear el directorio si no existe
                    os.makedirs(output_dir, exist_ok=True)

                    # Guardar el DataFrame en el archivo CSV
                    missing_dates_df.reset_index(inplace=True)
                    missing_dates_df.to_csv(csv_filename, index=False, columns=['id_estacion', 'codigo', 'nemonico', 'id_combinacion_parametro', 'fechas_faltantes'])
                else:
                    print(f"No hay fechas faltantes para la estación {station} y combinación {combination}")

if __name__ == "__main__":
    main()
