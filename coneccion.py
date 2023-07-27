import psycopg2

# coneccion.py

def coneccion_postgres():
    # Especificar los detalles de conexión a la base de datos PostgreSQL
    host = "localhost"
    database = "bandahm"
    user = "postgres"
    password = "postgres"

    # Crear la cadena de conexión
    connection_string = f"postgresql://{user}:{password}@{host}/{database}"

    return connection_string
