"""
Enunciado:
En este ejercicio aprenderás a utilizar MongoDB con Python para trabajar
con bases de datos NoSQL. MongoDB es una base de datos orientada a documentos que
almacena datos en formato similar a JSON (BSON).

Tareas:
1. Conectar a una base de datos MongoDB
2. Crear colecciones (equivalentes a tablas en SQL)
3. Insertar, actualizar, consultar y eliminar documentos
4. Manejar transacciones y errores

Este ejercicio se enfoca en las operaciones básicas de MongoDB desde Python utilizando PyMongo.
"""

import subprocess
import time
import os
import sys
from typing import List, Tuple, Optional

import pymongo
from bson.objectid import ObjectId

# Configuración de MongoDB (la debes obtener de "docker-compose.yml"):
DB_NAME = "biblioteca"
MONGODB_PORT = 27017
MONGODB_HOST = 'localhost'
MONGODB_USERNAME = 'testuser'
MONGODB_PASSWORD = 'testpass'

def verificar_docker_instalado() -> bool:
    """
    Verifica si Docker está instalado en el sistema y el usuario tiene permisos
    """
    try:
        # Verificar si docker está instalado
        result = subprocess.run(["docker", "--version"],
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE,
                               text=True)
        if result.returncode != 0:
            return False

        # Verificar si docker-compose está instalado
        result = subprocess.run(["docker", "compose", "version"],
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE,
                               text=True)
        if result.returncode != 0:
            return False

        # Verificar permisos de Docker
        result = subprocess.run(["docker", "ps"],
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE,
                               text=True)
        return result.returncode == 0
    except FileNotFoundError:
        return False

def iniciar_mongodb_docker() -> bool:
    """
    Inicia MongoDB usando Docker Compose
    """
    try:
        # Obtener la ruta al directorio actual donde está el docker-compose.yml
        current_dir = os.path.dirname(os.path.abspath(__file__))

        # Detener cualquier contenedor previo
        subprocess.run(
            ["docker", "compose", "down"],
            cwd=current_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True
        )

        # Iniciar MongoDB con docker-compose
        result = subprocess.run(
            ["docker", "compose", "up", "-d"],
            cwd=current_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        if result.returncode != 0:
            print(f"Error al iniciar MongoDB: {result.stderr}")
            return False

        # Dar tiempo para que MongoDB se inicie completamente
        time.sleep(5)
        return True

    except subprocess.CalledProcessError as e:
        print(f"Error al ejecutar Docker Compose: {e}")
        return False
    except Exception as e:
        print(f"Error inesperado: {e}")
        return False

def detener_mongodb_docker() -> None:
    """
    Detiene el contenedor de MongoDB
    """
    try:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        subprocess.run(
            ["docker", "compose", "down"],
            cwd=current_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True
        )
    except Exception as e:
        print(f"Error al detener MongoDB: {e}")

def crear_conexion() -> pymongo.database.Database:
    """
    Crea y devuelve una conexión a la base de datos MongoDB
    """
    # Debes conectarte a la base de datos MongoDB usando PyMongo
    #pass
    # Conectamos haciendo una instancia de MONGODB
    mongo_client = pymongo.MongoClient(host = MONGODB_HOST, port = MONGODB_PORT, username = MONGODB_USERNAME, password = MONGODB_PASSWORD)
    # Elegimos la base de datos como si fuera un diccionario.
    database = mongo_client[DB_NAME]
    return database

def crear_colecciones(db: pymongo.database.Database) -> None:
    """
    Crea las colecciones necesarias para la biblioteca.
    En MongoDB, no es necesario definir el esquema de antemano,
    pero podemos crear índices para optimizar el rendimiento.
    """
    # Debes crear colecciones para 'autores' y 'libros'
    # 1. Crear colección de autores con índice por nombre
    # 2. Crear colección de libros con índices
    #pass
    # Especifico check_exists = False para que no pete el test que nos dan.
    db.create_collection('autores', check_exists= False)
    db.autores.create_index('nombre')
    db.create_collection('libros', check_exists= False)
    db.libros.create_index('nombre')

def insertar_autores(db: pymongo.database.Database, autores: List[Tuple[str]]) -> List[str]:
    """
    Inserta varios autores en la colección 'autores'
    """
    # Debes realizar los siguientes pasos:
    # 1. Convertir las tuplas a documentos
    # 2. Insertar los documentos
    # 3. Devolver los IDs como strings
    # Para crear coleccion usamos el metodo insert_many de la db. y le pasamos el array de diccionarios.
    list_autores = []
    for autor in autores:
        list_autores.append({'nombre' : autor[0]})
    ids = db.autores.insert_many(list_autores)
    #for id in ids:
    #    print (f'id: {id}')
    #print(f'IDS: {ids.inserted_ids}')
    return ids.inserted_ids

def insertar_libros(db: pymongo.database.Database, libros: List[Tuple[str, int, str]]) -> List[str]:
    """
    Inserta varios libros en la colección 'libros'
    """
    # Debes realizar los siguientes pasos:
    # 1. Convertir las tuplas a documentos
    # 2. Insertar los documentos
    # 3. Devolver los IDs como strings
    #pass
    list_libros = []
    for libro in libros:
        list_libros.append({'titulo' : libro[0] , 'anio' : libro[1], 'autor_id' : libro[2]})

    ids = db.libros.insert_many(list_libros)
    #for id in ids:
    #    print (f'id: {id}')
    #print(f'IDS: {ids.inserted_ids}')
    return ids.inserted_ids


def consultar_libros(db: pymongo.database.Database) -> None:
    """
    Consulta todos los libros y muestra título, año y nombre del autor
    """
    # Debes realizar los siguientes pasos:
    # 1. Realizar una agregación para unir libros con autores
    # 2. Mostrar los resultados
    #pass
    #db.get_collection('libros')
    # Se hace una agregacion y se le pasa un diccionario con lo que se quiere hacer. 
    # El foreign key es la columna de libros que queremos vincular
    # localField es el campo del documento "from"

    res = db.libros.aggregate([
        {
            "$lookup" : {
                "from" : "autores",
                "foreignField" : "autor_id",
                "localField" : "nombre",
                "as" : "autor_id"
            }
        }

    ])
    
    print(f"{res.to_list()}")

def buscar_libros_por_autor(db: pymongo.database.Database, nombre_autor: str) -> List[Tuple[str, int]]:
    """
    Busca libros por el nombre del autor
    """
    # Debes realizar los siguientes pasos:
    # 1. Primero encontrar el autor y buscar todos los libros del autor
    # 2. Convertir a lista de tuplas (titulo, anio)
    #pass
    # Primero busco el autor.
    list_libros = []
    docs_autor = db.autores.find({"nombre" : nombre_autor})
    for doc_autor in docs_autor:
        # Primero es la query., el segundo son los  campos que queremos que devuelva
#        docs_libro = db.libros.find({"autor_id" : doc_autor['nombre']}, { 'titulo' : 1 , 'anio' : 1 , 'autor_id' : 0})
        #print(f"ASDASDASDAS: {doc_autor}, asdasd : {doc_autor['nombre']}")
        # En el autor_id se guarda el _id del documento autor.
        docs_libro = db.libros.find({"autor_id" : doc_autor['_id']})
        for doc_libro in docs_libro:
            list_libros.append((doc_libro['titulo'], doc_libro['anio']))
    return list_libros

def actualizar_libro(
        db: pymongo.database.Database,
        id_libro: str,
        nuevo_titulo: Optional[str]=None,
        nuevo_anio: Optional[int]=None
) -> bool:
    """
    Actualiza la información de un libro existente
    """
    # Debes realizar los siguientes pasos:
    # 1. Crear diccionario de actualización
    # 2. Realizar la actualización
    # el diccionario de update ha de tener la clave $set para indicar  los valores que acutalizamos.
    update_dict = {}
    if nuevo_titulo is not None:
        update_dict['titulo'] = nuevo_titulo
    if nuevo_anio is not None:
        update_dict['anio'] = nuevo_anio
    # Nos pasan el object id en string por lo que para hacer la busqueda, hay que hacer un cast a ObjectId
    res = db.libros.update_many(filter= {'_id' : ObjectId(id_libro)}, update= { '$set' : update_dict})
    return res.matched_count == res.modified_count


def eliminar_libro(
        db: pymongo.database.Database,
        id_libro: str
) -> bool:
    """
    Elimina un libro por su ID
    """
    # Debes eliminar el libro con el ID proporcionado
    #pass
    res = db.libros.delete_many(filter = {'_id' : ObjectId(id_libro)})
    return res.deleted_count > 0

def ejemplo_transaccion(db: pymongo.database.Database) -> bool:
    """
    Demuestra el uso de operaciones agrupadas
    """
    # Debes realizar los siguientes pasos:
    # 1. Insertar un nuevo autor
    # 2. Insertar dos libros del autor
    # Intentar limpiar los datos en caso de error
    #pass
    # Primero hemos de crear la sesion, podemos hacerlo desde la conexion , la podemos recuperar desde db con client.
    with db.client.start_session() as session:
        # creamos la transaccion
        session.start_transaction()
        try:
            res = db.autores.insert_one(document = {'nombre' : 'Pimpollo'})
            if res == None:
                raise Exception('Error inserting one document')
            res = db.libros.insert_many(documents = [{'titulo' : 'C', 'anio' : 1982 , 'autor_id' : 'a'},{'titulo' : 'D', 'anio' : 1922 , 'autor_id' : 'b'}])
            if res.inserted_ids is None or not res.inserted_ids:
                raise Exception('Error inserting many documents') 
            # hacemos commmit.
            session.commit_transaction()    
        except Exception as e:
            # Hubieron problemas, abortamos la transaccion.
            session.abort_transaction()
            return False
        return True


if __name__ == "__main__":
    mongodb_proceso = None
    db = None

    try:
        # Verificar si Docker está instalado
        if not verificar_docker_instalado():
            print("Error: Docker no está instalado o no está disponible en el PATH.")
            print("Por favor, instala Docker y asegúrate de que esté en tu PATH.")
            sys.exit(1)

        # Iniciar MongoDB usando Docker
        print("Iniciando MongoDB con Docker...")
        if not iniciar_mongodb_docker():
            print("No se pudo iniciar MongoDB. Asegúrate de tener los permisos necesarios.")
            sys.exit(1)

        print("MongoDB iniciado correctamente.")

        # Crear una conexión
        print("Conectando a MongoDB...")
        db = crear_conexion()
        print("Conexión establecida correctamente.")

        # TODO: Implementar el código para probar las funciones
        crear_colecciones(db)
        insertar_autores(db, [{'nombre' : 'Gabriel'}, {'nombre' : 'Gregorio'}])
        insertar_libros(db, [{'name' : 'El quijote' }])


    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Cerrar la conexión a MongoDB
        if db is None:
            print("\nConexión a MongoDB cerrada.")

        # Detener el proceso de MongoDB si lo iniciamos nosotros
        if mongodb_proceso:
            print("Deteniendo MongoDB...")
            detener_mongodb_docker()

            print("MongoDB detenido correctamente.")
