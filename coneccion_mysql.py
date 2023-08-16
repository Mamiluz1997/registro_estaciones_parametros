import mysql.connector

# Configura los parámetros de conexión a la base de datos
config = {
    'user': 'tu_usuario',
    'password': 'tu_contraseña',
    'host': 'localhost',
    'database': 'nombre_de_la_base_de_datos',
}

# Conecta a la base de datos
try:
    connection = mysql.connector.connect(**config)
    if connection.is_connected():
        print('Conexión exitosa a la base de datos')
except mysql.connector.Error as e:
    print('Error al conectarse a la base de datos:', e)
finally:
    if 'connection' in locals():
        connection.close()
        print('Conexión cerrada')
