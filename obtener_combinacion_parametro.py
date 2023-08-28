from sqlalchemy import create_engine
import pandas as pd
from datetime import timedelta
import coneccion_postgres

def main():
    # Obtener la cadena de conexión
    connection_string = coneccion_postgres.coneccion_postgres()

    # Crear el objeto Engine utilizando SQLAlchemy
    engine = create_engine(connection_string)

    # Consulta SQL para obtener los registros
    select_query = """
        SELECT dataf__id, datafetd
        FROM "storage".data_m01
        ORDER BY dataf__id, datafetd limit 10000;
    """

    # Utilizar Pandas para cargar los datos desde la base de datos
    df = pd.read_sql_query(select_query, engine)

    # Agrupar y analizar los datos con Pandas
    grouped = df.groupby('dataf__id')['datafetd']

    # Crear diccionarios para almacenar fechas existentes y faltantes por dataf__id
    existing_dates_by_id = {}
    missing_dates_by_id = {}

    for dataf__id, dates in grouped:
        min_date = dates.min()
        max_date = dates.max()
        all_dates = pd.date_range(start=min_date, end=max_date, freq='T')
        missing_dates = all_dates[~all_dates.isin(dates)]
        missing_dates_by_id[dataf__id] = missing_dates
        existing_dates = all_dates[all_dates.isin(dates)]
        existing_dates_by_id[dataf__id] = existing_dates

        # Imprimir fechas existentes por dataf__id
        for existing_date in existing_dates:
            print(f"Fechas existentes para {dataf__id} ({existing_date}):")

        # Imprimir fechas faltantes por dataf__id
        print(f"Fechas faltantes para {dataf__id} ({len(missing_dates)} fechas faltantes):")
        for missing_date in missing_dates:
            print(missing_date)

        print(f"Total de fechas faltantes para {dataf__id}: {len(missing_dates)}\n")

if __name__ == "__main__":
    main()
