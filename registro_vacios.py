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
            ac.copa__id AS id_copa,
            dm.datafetd AS fecha_existente
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
    df['fecha_existente'] = pd.to_datetime(df['fecha_existente'])

    # Obtener la lista de estaciones únicas
    unique_stations = df['id_estacion'].unique()

    for station in unique_stations:
        station_df = df[df['id_estacion'] == station]

        # Crear directorio si no existe
        output_dir = f'estacion_{station}'
        os.makedirs(output_dir, exist_ok=True)

        # Guardar el DataFrame en un archivo CSV
        csv_filename = f'{output_dir}/estacion_fechas_existentes_{station}.csv'
        station_df.to_csv(csv_filename, index=False)

if __name__ == "__main__":
    main()
