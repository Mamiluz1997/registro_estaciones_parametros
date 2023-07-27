import pandas as pd
from sqlalchemy import create_engine
import coneccion  # Importa el archivo coneccion.py donde se encuentra la función
import obtener_nemonico
import psycopg2

def obtener_fecha_por_nemonicos(nemonico):
    try:
        # Obtener la cadena de conexión a la base de datos PostgreSQL
        connection_string = coneccion.coneccion_postgres()

        # Crear el objeto Engine utilizando SQLAlchemy
        engine = create_engine(connection_string)

        # Especificar el nombre de la tabla desde la que deseas realizar el SELECT
        table_name = "estaciones_parametros"

        # Ejecutar la consulta SELECT para obtener las fechas de inicio y fin por nemonico
        query = f"SELECT nemonico, fecha_inicio, fecha_fin FROM administrativo.estaciones_parametros WHERE nemonico = '{nemonico}';"
        df = pd.read_sql_query(query, engine)

        return df

    except Exception as error:
        print(f"Error al obtener fechas para el nemonico {nemonico}:", error)
        return None

def insertar_fechas_en_tabla_vacios(df):
    try:
        # Obtener la cadena de conexión a la base de datos PostgreSQL
        connection_string = coneccion.coneccion_postgres()

        # Establecer la conexión con la base de datos PostgreSQL
        conexion = psycopg2.connect(connection_string)

        # Crear un cursor para ejecutar las consultas
        cursor = conexion.cursor()

        # Iterar sobre el DataFrame para obtener las fechas y realizar las inserciones
        for index, row in df.iterrows():
            nemonico = row['nemonico']
            fecha_inicio = row['fecha_inicio']
            fecha_fin = row['fecha_fin']

            # Crear la consulta SQL para insertar los datos en la tabla_vacios
            consulta = f"INSERT INTO administrativo.tabla_vacios (nemonico, fecha_inicio, fecha_fin) VALUES (%s, %s, %s);"

            # Ejecutar la consulta, pasando los valores correspondientes (incluyendo valores nulos)
            cursor.execute(consulta, (nemonico, fecha_inicio, fecha_fin))

        # Confirmar la transacción y cerrar el cursor y la conexión
        conexion.commit()
        cursor.close()
        conexion.close()

        print("Fechas insertadas exitosamente en la tabla_vacios.")

    except (Exception, psycopg2.Error) as error:
        print("Error al insertar fechas en la tabla_vacios:", error)

if __name__ == "__main__":
    nemonico_lista = obtener_nemonico.get_all_nemonicos()  # Obtiene la lista de nemonicos desde obtener_nemonico
    for nemonico in nemonico_lista:
        df = obtener_fecha_por_nemonicos(nemonico)
        if df is not None:
            print(f"Fechas para el nemonico {nemonico}:")
            print(df)
            insertar_fechas_en_tabla_vacios(df)
            print()  # Agrega un espacio en blanco para separar las salidas en la consola
