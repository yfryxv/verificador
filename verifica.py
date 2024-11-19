import mysql.connector

def conectar_bd(host, usuario, contraseña, bd):
    return mysql.connector.connect(
        host=host,
        user=usuario,
        password=contraseña,
        database=bd
    )

def sincronizar_datos(config_segundo, config_primero, tabla_segundo, tabla_primero):
    # Conectar a las bases de datos
    conexion_segundo = conectar_bd(**config_segundo)
    conexion_primero = conectar_bd(**config_primero)

    cursor_segundo = conexion_segundo.cursor(dictionary=True)
    cursor_primero = conexion_primero.cursor(dictionary=True)

    # Obtener registros de la tabla en SEGUNDO
    cursor_segundo.execute(f"SELECT * FROM {tabla_segundo}")
    registros_segundo = cursor_segundo.fetchall()

    for registro in registros_segundo:
        ruc = registro['ruc']

        # Buscar si el RUC ya existe en la base de datos PRIMERO
        cursor_primero.execute(f"SELECT * FROM {tabla_primero} WHERE ruc = %s", (ruc,))
        registro_primero = cursor_primero.fetchone()

        if registro_primero:
            # Compara los registros para verificar si hay diferencias
            if registro != registro_primero:
                # Actualiza si los datos son diferentes
                columnas = ", ".join([f"{col} = %s" for col in registro.keys() if col != 'id'])
                valores = [registro[col] for col in registro.keys() if col != 'id']
                cursor_primero.execute(
                    f"UPDATE {tabla_primero} SET {columnas} WHERE ruc = %s",
                    valores + [ruc]
                )
                print(f"Actualizado RUC {ruc} en la base de datos PRIMERO.")
        else:
            # Inserta si el RUC no existe
            columnas = ", ".join(registro.keys())
            valores_placeholder = ", ".join(["%s"] * len(registro))
            valores = list(registro.values())
            cursor_primero.execute(
                f"INSERT INTO {tabla_primero} ({columnas}) VALUES ({valores_placeholder})",
                valores
            )
            print(f"Insertado RUC {ruc} en la base de datos PRIMERO.")

    # Confirmar cambios
    conexion_primero.commit()

    # Cerrar conexiones
    cursor_segundo.close()
    cursor_primero.close()
    conexion_segundo.close()
    conexion_primero.close()

# Configuración de conexión para las bases de datos
config_bd_segundo = {
    'host': 'localhost',
    'usuario': 'root',
    'contraseña': '150405',
    'bd': 'SEGUNDO'
}

config_bd_primero = {
    'host': 'localhost',
    'usuario': 'root',
    'contraseña': '150405',
    'bd': 'PRIMERO'
}

# Llamada a la función para sincronizar
sincronizar_datos(config_bd_segundo, config_bd_primero, 'ruc_tables1', 'ruc_tabless')
