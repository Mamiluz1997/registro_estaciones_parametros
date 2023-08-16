import pandas as pd
from sqlalchemy import create_engine
import coneccion_postgres
import obtener_combinacion_parametro

def obtener_fecha_por_copa__id(copa__id):
    try:
        # Obtener la cadena de conexión a la base de datos PostgreSQL
        connection_string = coneccion_postgres.coneccion_postgres()

        # Crear el objeto Engine utilizando SQLAlchemy
        engine = create_engine(connection_string)

        # Especificar el nombre de la tabla desde la que deseas realizar el SELECT
        table_name = "data_m01"

        # Ejecutar la consulta SELECT para obtener las fechas de inicio y fin por nemonico
        query = f"SELECT copa__id, datafetd FROM storage.data_m01 WHERE copa__id = '{copa__id}' ORDER BY datafetd ASC LIMIT 50;"
        df = pd.read_sql_query(query, engine)

        # Agregar una columna "fecha_fin" con la siguiente fecha en la secuencia
        df["fecha_fin"] = df["datafetd"].shift(-1)

        return df

    except Exception as error:
        print(f"Error al obtener fechas para copa__id {copa__id}:", error)
        return None

if __name__ == "__main__":
    lista_combinacion_parametro = obtener_combinacion_parametro.get_all_copa__id()  # Llama a la función para obtener la lista
    for copa__id in lista_combinacion_parametro:
        df = obtener_fecha_por_copa__id(copa__id)
        if df is not None:
            print(f"Fechas para copa_id {copa__id}:")
            print(df.head(50))  # Muestra solo los primeros 100 resultados
            print()  # Agrega un espacio en blanco para separar las salidas en la consola
