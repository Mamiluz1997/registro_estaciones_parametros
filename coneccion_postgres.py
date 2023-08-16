import psycopg2

# coneccion.py

def coneccion_postgres():
    # Especificar los detalles de conexión a la base de datos PostgreSQL
    host = "192.168.1.226"
    database = "bandahm"
    user = "postgres"
    password = "inamhidb"

    # Crear la cadena de conexión
    connection_string = f"postgresql://{user}:{password}@{host}/{database}"

    return connection_string
