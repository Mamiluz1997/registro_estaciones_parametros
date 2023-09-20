import pandas as pd
from sqlalchemy import create_engine, text

# Parámetros de conexión a la base de datos original
orig_db_params = {
    'username': 'tu_usuario_original',
    'password': 'tu_contraseña_original',
    'host': 'tu_host_original',
    'database': 'tu_database_original'
}

# Parámetros de conexión a la base de datos nueva
new_db_params = {
    'username': 'tu_usuario_nuevo',
    'password': 'tu_contraseña_nuevo',
    'host': 'tu_host_nuevo',
    'database': 'tu_database_nuevo'
}

# Cargar los esta__id desde el archivo CSV
csv_path = 'estaciones.csv'  
esta__id_df = pd.read_csv(csv_path)

# Iterar a través de cada esta__id y calcular el total de datos
for esta__id in esta__id_df['esta__id']:
    # Crear la conexión al motor SQLAlchemy para la base de datos original
    orig_engine = create_engine(f'postgresql://{orig_db_params["username"]}:{orig_db_params["password"]}@{orig_db_params["host"]}/{orig_db_params["database"]}')

    # Obtener el código de la estación desde vta_estaciones_todos
    with orig_engine.begin() as connection:
        codigo_query = text(f"""
            SELECT puobcodi FROM administrativo.vta_estaciones_todos WHERE esta__id = {esta__id}
        """)
        codigo_result = connection.execute(codigo_query).fetchone()
        codigo = codigo_result[0] if codigo_result else None

    # Obtener el nemonico desde la tabla copa
    with orig_engine.begin() as connection:
        nemonico_query = text(f"""
            SELECT copanemo FROM administrativo.copa WHERE copa__id = (
                SELECT copa__id FROM "storage".data_m03 WHERE esta__id = {esta__id} LIMIT 1
            )
        """)
        nemonico_result = connection.execute(nemonico_query).fetchone()
        nemonico = nemonico_result[0] if nemonico_result else None

    if codigo is not None and nemonico is not None:
        # Crear la conexión al motor SQLAlchemy para la base de datos nueva
        new_engine = create_engine(f'postgresql://{new_db_params["username"]}:{new_db_params["password"]}@{new_db_params["host"]}/{new_db_params["database"]}')

        query = f"""
            SELECT esta__id, copa__id, COUNT(*) AS total_datos
            FROM "storage".data_m03
            WHERE esta__id = {esta__id}
            GROUP BY esta__id, copa__id
        """
        esta__id_result_df = pd.read_sql(query, orig_engine)

        # Iniciar una transacción para manejar inserciones y actualizaciones en la nueva base de datos
        with new_engine.begin() as conn:
            for index, row in esta__id_result_df.iterrows():
                id_estacion = int(row['esta__id'])  # Convertir a tipo int
                id_combinacion_parametro = int(row['copa__id'])  # Convertir a tipo int
                total_datos = int(row['total_datos'])  # Convertir a tipo int
                
                # Actualizar o insertar el registro en la tabla estadisticos_historial en la nueva base de datos
                upsert_query = text("""
                    INSERT INTO administrativo.estadisticos_historial (id_estacion, id_combinacion_parametro, total_datos, codigo, nemonico)
                    VALUES (:id_estacion, :id_combinacion_parametro, :total_datos, :codigo, :nemonico)
                    ON CONFLICT (id_estacion, id_combinacion_parametro) DO UPDATE
                    SET total_datos = administrativo.estadisticos_historial.total_datos + :total_datos
                """)
                
                conn.execute(upsert_query, {
                    'id_estacion': id_estacion,
                    'id_combinacion_parametro': id_combinacion_parametro,
                    'total_datos': total_datos,
                    'codigo': codigo,
                    'nemonico': nemonico
                })

print("Los valores en la tabla estadisticos_historial en la base de datos han sido actualizados o insertados según sea necesario.")
