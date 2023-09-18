import pandas as pd
from sqlalchemy import create_engine, text

# Conexión a la base de datos
# Reemplaza 'usuario', 'contraseña', 'localhost', 'basedatos' con tus propias credenciales
engine = create_engine('postgresql://postgres:postgres@localhost/bandahm')

# Cargar los esta__id desde el archivo CSV
csv_path = 'estaciones.csv'  # Reemplaza 'archivo.csv' con la ruta de tu archivo CSV
esta__id_df = pd.read_csv(csv_path)

# Iterar a través de cada esta__id y calcular el total de datos
for esta__id in esta__id_df['esta__id']:
    # Obtener el código de la estación desde vta_estaciones_todos
    connection = engine.connect()  # Crear una conexión
    codigo_query = text(f"""
        SELECT puobcodi FROM administrativo.vta_estaciones_todos WHERE esta__id = {esta__id}
    """)
    codigo_result = connection.execute(codigo_query).fetchone()
    connection.close()  # Cerrar la conexión
    codigo = codigo_result[0] if codigo_result else None

    # Obtener el nemonico desde la tabla copa
    connection = engine.connect()  # Crear una conexión
    nemonico_query = text(f"""
        SELECT copanemo FROM administrativo.copa WHERE copa__id = (
            SELECT copa__id FROM "storage".data_m01 WHERE esta__id = {esta__id} LIMIT 1
        )
    """)
    nemonico_result = connection.execute(nemonico_query).fetchone()
    connection.close()  # Cerrar la conexión
    nemonico = nemonico_result[0] if nemonico_result else None

    if codigo is not None and nemonico is not None:
        query = f"""
            SELECT esta__id, copa__id, COUNT(*) AS total_datos
            FROM "storage".data_m01
            WHERE esta__id = {esta__id}
            GROUP BY esta__id, copa__id
        """
        esta__id_result_df = pd.read_sql(query, engine)

        # Iniciar una transacción para manejar inserciones y actualizaciones
        with engine.begin() as conn:
            for index, row in esta__id_result_df.iterrows():
                id_estacion = int(row['esta__id'])  # Convertir a tipo int
                id_combinacion_parametro = int(row['copa__id'])  # Convertir a tipo int
                total_datos = int(row['total_datos'])  # Convertir a tipo int
                
                # Actualizar o insertar el registro en la tabla prueba_historial
                upsert_query = text("""
                    INSERT INTO administrativo.prueba_historial (id_estacion, id_combinacion_parametro, total_datos, codigo, nemonico)
                    VALUES (:id_estacion, :id_combinacion_parametro, :total_datos, :codigo, :nemonico)
                    ON CONFLICT (id_estacion, id_combinacion_parametro) DO UPDATE
                    SET total_datos = administrativo.prueba_historial.total_datos + :total_datos
                """)
                
                conn.execute(upsert_query, {
                    'id_estacion': id_estacion,
                    'id_combinacion_parametro': id_combinacion_parametro,
                    'total_datos': total_datos,
                    'codigo': codigo,
                    'nemonico': nemonico
                })

print("Los valores en la tabla prueba_historial han sido actualizados o insertados según sea necesario.")
