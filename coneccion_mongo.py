from pymongo import MongoClient

def conectar_a_mongodb():
    try:
        # Crear una instancia del cliente de MongoDB
        client = MongoClient("mongodb://localhost:27017/")

        return client

    except Exception as e:
        print("Error al conectar a MongoDB:", str(e))
        return None

def main():
    # Conectar a MongoDB
    client = conectar_a_mongodb()

    if client is not None:
        print("Conexión exitosa a MongoDB.")
        # Puedes realizar más acciones con la conexión aquí si es necesario

        # Cerrar la conexión al finalizar
        client.close()
    else:
        print("No se pudo conectar a MongoDB.")

if __name__ == "__main__":
    main()
