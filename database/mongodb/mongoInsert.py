from pymongo import MongoClient
import json

def insertar_datos_mongodb():
    # Conexión a MongoDB
    client = MongoClient('mongodb://localhost:27017/')
    db = client['bda']
    collection = db['clientes']

    # Leer el archivo JSON de clientes
    with open('../..'
              '/data/mongo/clientes.json', 'r') as file:
        clientes_data = file.read()
    clientes_data = json.loads(clientes_data)
    collection.insert_many(clientes_data)

    print("Datos insertados exitosamente en MongoDB")

if __name__ == "__main__":
    insertar_datos_mongodb()
