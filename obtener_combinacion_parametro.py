from sqlalchemy import create_engine
import pandas as pd
import coneccion_postgres
import os

def main():
    # Obtener la cadena de conexión
    connection_string = coneccion_postgres.coneccion_postgres()

    # Crear el objeto Engine utilizando SQLAlchemy
    engine = create_engine(connection_string)

    # Consulta SQL para obtener los registros
    select_query = """
        SELECT 
            dm.esta__id AS id_estacion,
            ac.copanemo AS nemonico,
            ac.copa__id AS id_combinacion_parametro,
            dm.datafetd AS fecha_faltante
        FROM 
            "storage".data_m01 dm
        JOIN 
            "administrativo".copa ac ON dm.esta__id::text = ac.copa__id::text
        ORDER BY 
            id_estacion, dm.datafetd;
    """

    # Utilizar Pandas para cargar los datos desde la base de datos
    df = pd.read_sql_query(select_query, engine)

    # Convertir la columna 'datafetd' a tipo datetime
    df['fecha_faltante'] = pd.to_datetime(df['fecha_faltante'])

    # Obtener la lista de estaciones únicas
    unique_stations = df['id_estacion'].unique()

    for station in unique_stations:
        station_df = df[df['id_estacion'] == station]

        # Convertir la columna 'fecha_faltante' en un índice para facilitar la comparación
        station_df.set_index('fecha_faltante', inplace=True)

        # Crear un rango de fechas completo para esa estación con la frecuencia adecuada
        date_range = pd.date_range(start=station_df.index.min(), end=station_df.index.max(), freq='T')

        # Encontrar las fechas faltantes
        missing_dates = date_range[~date_range.isin(station_df.index)]

        # Crear un DataFrame de fechas faltantes con todas las columnas necesarias
        missing_dates_df = pd.DataFrame(data={'fechas_faltantes': missing_dates})
        missing_dates_df['id_estacion'] = station_df['id_estacion'][0]
        missing_dates_df['nemonico'] = station_df['nemonico'][0]
        missing_dates_df['id_combinacion_parametro'] = station_df['id_combinacion_parametro'][0]

        # Si hay fechas faltantes, crear directorio si no existe y guardar el DataFrame en un archivo CSV
        if not missing_dates_df.empty:
            output_dir = f'estacion_{station}'
            os.makedirs(output_dir, exist_ok=True)

            csv_filename = f'{output_dir}/estacion_{station}.csv'
            missing_dates_df.reset_index(inplace=True)
            missing_dates_df.to_csv(csv_filename, index=False, columns=['id_estacion', 'nemonico', 'id_combinacion_parametro', 'fechas_faltantes'])

if __name__ == "__main__":
    main()
