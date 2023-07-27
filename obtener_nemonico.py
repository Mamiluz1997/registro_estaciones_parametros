import pandas as pd
from sqlalchemy import create_engine
import coneccion  # Importa el archivo coneccion.py donde se encuentra la función

def get_all_nemonicos():
    try:
        # Obtener la cadena de conexión a la base de datos PostgreSQL
        connection_string = coneccion.coneccion_postgres()

        # Crear el objeto Engine utilizando SQLAlchemy
        engine = create_engine(connection_string)

        # Especificar el nombre de la tabla desde la que deseas obtener los nemonicos
        table_name = "estaciones_parametros"

        # Ejecutar la consulta SELECT para obtener todos los nemonicos
        query = f"SELECT DISTINCT nemonico FROM administrativo.estaciones_parametros;"
        df = pd.read_sql_query(query, engine)

        # Obtener los nemonicos como una lista
        nemonicos_list = df["nemonico"].tolist()

        return nemonicos_list

    except Exception as error:
        print("Error al conectarse o interactuar con la base de datos:", error)
        return []

if __name__ == "__main__":
    nemonicos_list = get_all_nemonicos()
    print("Todos los nemonicos:")
    print(nemonicos_list)
