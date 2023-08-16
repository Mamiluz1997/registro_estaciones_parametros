import pandas as pd
from sqlalchemy import create_engine
import coneccion_postgres  # Importa el archivo coneccion.py donde se encuentra la función

def get_all_copa__id():
    try:
        # Obtener la cadena de conexión a la base de datos PostgreSQL
        connection_string = coneccion_postgres.coneccion_postgres()

        # Crear el objeto Engine utilizando SQLAlchemy
        engine = create_engine(connection_string)

        # Especificar el nombre de la tabla desde la que deseas obtener los copa_id_list
        table_name = "data_m01"

        # Ejecutar la consulta SELECT para obtener todos los copa_id_list
        query = f"SELECT DISTINCT copa__id FROM storage.data_m01 LIMIT 50;"
        df = pd.read_sql_query(query, engine)

        # Obtener los copa_id_list como una lista
        copa_id_list = df["copa__id"].tolist()

        return copa_id_list

    except Exception as error:
        print("Error al conectarse o interactuar con la base de datos:", error)
        return []

if __name__ == "__main__":
    copa_id_list = get_all_copa__id()
    print("id combinacion parametros:")
    print(copa_id_list)
