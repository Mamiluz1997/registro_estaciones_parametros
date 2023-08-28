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

        # Ejecutar la consulta SELECT para obtener las fechas de inicio y fin por copa__id
        query = f"SELECT copa__id, datafetd FROM storage.data_m01 WHERE copa__id = {copa__id} ORDER BY datafetd ASC;"
        df = pd.read_sql_query(query, engine)

        # Agregar una columna "fecha_fin" con la siguiente fecha en la secuencia
        df["fecha_fin"] = df["datafetd"].shift(-1)

        return df

    except Exception as error:
        print(f"Error al obtener fechas para copa__id {copa__id}:", error)
        return None

if __name__ == "__main__":

    #missing_ids_df = obtener_combinacion_parametro.compare_copa_ids()  # Obtener los "copa__id" faltantes

    # Convertir los elementos de la columna "copa__id" a enteros
   # missing_ids = missing_ids_df["copa__id"].astype(int)
    
    lista_combinacion_parametro = obtener_combinacion_parametro.compare_copa_ids()

    # Crear un DataFrame para almacenar resultados
    results_df = pd.DataFrame(columns=["copa__id", "datafetd", "fecha_fin"])

    missing_ids = []

    for copa__id in lista_combinacion_parametro:
        df = obtener_fecha_por_copa__id(copa__id)
        if df is not None:
            # Agregar las filas a results_df
            results_df = pd.concat([results_df, df], ignore_index=True)
        else:
            # Agregar a la lista de datos faltantes
            missing_ids.append(copa__id)

    print("Resultados:")
    print(results_df)

    print("\nDatos faltantes:")
    #print(missing_ids_df)
    #print("Total de datos faltantes:", len(missing_ids_df))