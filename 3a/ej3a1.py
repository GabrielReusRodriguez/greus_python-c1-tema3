"""
Enunciado:
En este ejercicio aprenderás a utilizar la biblioteca sqlite3 de Python para trabajar
con bases de datos SQLite. SQLite es una base de datos liviana que no requiere un servidor
y almacena la base de datos completa en un solo archivo.

Tareas:
1. Conectar a una base de datos SQLite
2. Crear tablas usando SQL
3. Insertar, actualizar, consultar y eliminar datos
4. Manejar transacciones y errores

Este ejercicio se enfoca en las operaciones básicas de SQL desde Python sin utilizar un ORM.
"""

import sqlite3
import os

# Ruta de la base de datos (en memoria para este ejemplo)
# Para una base de datos en archivo, usar: 'biblioteca.db'
DB_PATH = ':memory:'

def crear_conexion():
    """
    Crea y devuelve una conexión a la base de datos SQLite
    """
    # Implementa la creación de la conexión y retorna el objeto conexión
    #pass
    try:
        con = sqlite3.connect(DB_PATH)
    except Exception as e:
        con = None
    return con


def crear_tablas(conexion):
    """
    Crea las tablas necesarias para la biblioteca:
    - autores: id (entero, clave primaria), nombre (texto, no nulo)
    - libros: id (entero, clave primaria), titulo (texto, no nulo),
              anio (entero), autor_id (entero, clave foránea a autores.id)
    """
    # Implementa la creación de tablas usando SQL
    # Usa conexion.cursor() para crear un cursor y ejecutar comandos SQL
    #pass
    cursor = conexion.cursor()
    cursor.execute('create table if not exists autores (id integer PRIMARY KEY AUTOINCREMENT, nombre varchar2 NOT NULL)')
    cursor.execute('create table if not exists libros (id integer PRIMARY KEY AUTOINCREMENT, titulo varchar2 NOT NULL, anio integer, autor_id integer, FOREIGN KEY (autor_id) REFERENCES autor(id))')
    cursor.close()

def insertar_autores(conexion, autores):
    """
    Inserta varios autores en la tabla 'autores'
    Parámetro autores: Lista de tuplas (nombre,)
    """
    # Implementa la inserción de autores usando SQL INSERT
    # Usa consultas parametrizadas para mayor seguridad
    #pass
    cursor = conexion.cursor()
    for autor in autores:
        cursor.execute(f"insert into autores(nombre)  values ('{autor[0]}')")
    cursor.close()
    conexion.commit()


def insertar_libros(conexion, libros):
    """
    Inserta varios libros en la tabla 'libros'
    Parámetro libros: Lista de tuplas (titulo, anio, autor_id)
    """
    # Implementa la inserción de libros usando SQL INSERT
    # Usa consultas parametrizadas para mayor seguridad
    #pass
    cursor = conexion.cursor()
    for libro in libros:
        cursor.execute(f"insert into libros(titulo, anio, autor_id)  values ('{libro[0]}', {libro[1]}, {libro[2]})")
    cursor.close()
    conexion.commit()

def consultar_libros(conexion):
    """
    Consulta todos los libros y muestra título, año y nombre del autor
    """
    # Implementa una consulta SQL JOIN para obtener libros con sus autores
    # Imprime los resultados formateados
    cursor = conexion.cursor()
    cursor.execute('select * from libros')
    output = cursor.fetchall()
    for row in output:
        cursor2 = conexion.cursor()
        autores = cursor2.execute(f'select nombre from autores where id = {row[3]}')
        for autor in autores:
            autor_libro = autor[0]
        cursor2.close()
        print(f'id: {row[0]}\t titulo: {row[1]}\t titulo: {row[2]}, autor: {autor_libro}')
    cursor.close()


def buscar_libros_por_autor(conexion, nombre_autor):
    """
    Busca libros por el nombre del autor
    """
    # Implementa una consulta SQL con WHERE para filtrar por autor
    # Retorna una lista de tuplas (titulo, anio)
    #pass
    libros = []
    cursor = conexion.cursor()
    # Primero obtengo el id del autor.
    cursor.execute(f"SELECT id FROM autores WHERE nombre = '{nombre_autor}'")
    id_autor = cursor.fetchone()
    if id_autor is None:
        return []
    id_autor = id_autor[0]    
    cursor.execute(f'SELECT titulo, anio FROM libros WHERE autor_id = {id_autor}')
    output = cursor.fetchall()
    for row in output:
        libro = (row[0], row[1])
        libros.append(libro)
    cursor.close()
    
    return libros

def actualizar_libro(conexion, id_libro, nuevo_titulo=None, nuevo_anio=None):
    """
    Actualiza la información de un libro existente
    """
    # Implementa la actualización usando SQL UPDATE
    # Solo actualiza los campos que no son None
    #pass
    if nuevo_anio is None and nuevo_titulo is None:
        return

    statement = f'UPDATE  libros SET '
    if nuevo_titulo is not None:
        statement = statement + f' titulo = "{nuevo_titulo}"'
    if nuevo_anio is not None:
        if nuevo_titulo is not None:
            statement = statement + f' , '
        statement = statement + f' anio = {nuevo_anio}'
    statement = statement + f' WHERE id = {id_libro}'
    cursor = conexion.cursor()
    cursor.execute(statement)
    cursor.close()
    conexion.commit()



def eliminar_libro(conexion, id_libro):
    """
    Elimina un libro por su ID
    """
    # Implementa la eliminación usando SQL DELETE
    #pass
    if id_libro is None:
        return
    statement = f'DELETE FROM  libros WHERE id = {id_libro}'
    cursor = conexion.cursor()
    cursor.execute(statement)
    cursor.close()
    conexion.commit()


def ejemplo_transaccion(conexion):
    """
    Demuestra el uso de transacciones para operaciones agrupadas
    """
    # Implementa una transacción que:
    # 1. Comience con conexion.execute("BEGIN TRANSACTION")
    # 2. Realice varias operaciones
    # 3. Si todo está bien, confirma con conexion.commit()
    # 4. En caso de error, revierte con conexion.rollback()
    #pass
    conexion.execute('BEGIN TRANSACTION')
    cursor = conexion.cursor()
    cursor.execute('DELETE FROM libros')
    conexion.rollback()
    cursor.close()

if __name__ == "__main__":
    try:
        # Crear una conexión
        conexion = crear_conexion()

        print("Creando tablas...")
        crear_tablas(conexion)

        # Insertar autores
        autores = [
            ("Gabriel García Márquez",),
            ("Isabel Allende",),
            ("Jorge Luis Borges",)
        ]
        insertar_autores(conexion, autores)
        print("Autores insertados correctamente")

        # Insertar libros
        libros = [
            ("Cien años de soledad", 1967, 1),
            ("El amor en los tiempos del cólera", 1985, 1),
            ("La casa de los espíritus", 1982, 2),
            ("Paula", 1994, 2),
            ("Ficciones", 1944, 3),
            ("El Aleph", 1949, 3)
        ]
        insertar_libros(conexion, libros)
        print("Libros insertados correctamente")

        print("\n--- Lista de todos los libros con sus autores ---")
        consultar_libros(conexion)

        print("\n--- Búsqueda de libros por autor ---")
        nombre_autor = "Gabriel García Márquez"
        libros_autor = buscar_libros_por_autor(conexion, nombre_autor)
        print(f"Libros de {nombre_autor}:")
        for titulo, anio in libros_autor:
            print(f"- {titulo} ({anio})")

        print("\n--- Actualización de un libro ---")
        actualizar_libro(conexion, 1, nuevo_titulo="Cien años de soledad (Edición especial)")
        print("Libro actualizado. Nueva información:")
        consultar_libros(conexion)

        print("\n--- Eliminación de un libro ---")
        eliminar_libro(conexion, 6)  # Elimina "El Aleph"
        print("Libro eliminado. Lista actualizada:")
        consultar_libros(conexion)

        print("\n--- Demostración de transacción ---")
        ejemplo_transaccion(conexion)

    except sqlite3.Error as e:
        print(f"Error de SQLite: {e}")
    finally:
        if conexion:
            conexion.close()
            print("\nConexión cerrada.")
