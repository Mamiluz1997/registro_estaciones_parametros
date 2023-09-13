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
    query = f"""
        SELECT esta__id, copa__id, COUNT(*) AS total_datos
        FROM "storage".data_m02  -- Cambia el nombre de la tabla a la que quieras agregar
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
                INSERT INTO administrativo.prueba_historial (id_estacion, id_combinacion_parametro, total_datos)
                VALUES (:id_estacion, :id_combinacion_parametro, :total_datos)
                ON CONFLICT (id_estacion, id_combinacion_parametro) DO UPDATE
                SET total_datos = administrativo.prueba_historial.total_datos + :total_datos
            """)
            
            conn.execute(upsert_query, {
                'id_estacion': id_estacion,
                'id_combinacion_parametro': id_combinacion_parametro,
                'total_datos': total_datos
            })

print("Los valores en la tabla prueba_historial han sido actualizados o insertados según sea necesario.")
