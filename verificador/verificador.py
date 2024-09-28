from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime
from sqlalchemy.exc import SQLAlchemyError

# Crear la aplicación Flask
app = Flask(__name__)

# Configurar la conexión a las bases de datos
app.config['SQLALCHEMY_BINDS'] = {
    'apidatav1': 'mysql+pymysql://root:150405@localhost/apidatav1',
    'auxiliar_db1': 'mysql+pymysql://root:150405@localhost/auxiliar_db1'
}

app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# Inicializar SQLAlchemy
db = SQLAlchemy(app)

# Modelos de las tablas en la base de datos oficial
class DNIOficial(db.Model):
    __bind_key__ = 'apidatav1'
    __tablename__ = 'dni_auxiliar'

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    dni = db.Column(db.String(255), nullable=False, unique=True)
    nombres = db.Column(db.String(255), nullable=True)
    apellidoPaterno = db.Column(db.String(255), nullable=True)
    apellidoMaterno = db.Column(db.String(255), nullable=True)
    codVerifica = db.Column(db.String(255), nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, onupdate=datetime.utcnow)

class RUCOficial(db.Model):
    __bind_key__ = 'apidatav1'
    __tablename__ = 'ruc_auxiliar'

    id = db.Column(db.BigInteger, primary_key=True)
    ruc = db.Column(db.String(255), nullable=False, unique=True)
    razonSocial = db.Column(db.String(255), nullable=True)
    direccion = db.Column(db.String(255), nullable=True)
    departamento = db.Column(db.String(255), nullable=True)
    provincia = db.Column(db.String(255), nullable=True)
    distrito = db.Column(db.String(255), nullable=True)
    ubigeo = db.Column(db.String(255), nullable=True)
    estado = db.Column(db.String(255), nullable=True)
    condicion = db.Column(db.String(255), nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, onupdate=datetime.utcnow)

# Modelos de las tablas en la base de datos externa
class DNIExterno(db.Model):
    __bind_key__ = 'auxiliar_db1'
    __tablename__ = 'dni_auxiliar1'

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    dni = db.Column(db.String(255), nullable=False, unique=True)
    nombres = db.Column(db.String(255), nullable=True)
    apellidoPaterno = db.Column(db.String(255), nullable=True)
    apellidoMaterno = db.Column(db.String(255), nullable=True)
    codVerifica = db.Column(db.String(255), nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, onupdate=datetime.utcnow)

class RUCExterno(db.Model):
    __bind_key__ = 'auxiliar_db1'
    __tablename__ = 'ruc_auxiliar1'

    id = db.Column(db.BigInteger, primary_key=True)
    ruc = db.Column(db.String(255), nullable=False, unique=True)
    razonSocial = db.Column(db.String(255), nullable=True)
    direccion = db.Column(db.String(255), nullable=True)
    departamento = db.Column(db.String(255), nullable=True)
    provincia = db.Column(db.String(255), nullable=True)
    distrito = db.Column(db.String(255), nullable=True)
    ubigeo = db.Column(db.String(255), nullable=True)
    estado = db.Column(db.String(255), nullable=True)
    condicion = db.Column(db.String(255), nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, onupdate=datetime.utcnow)

# Función para verificar y agregar datos del DNI
def verificar_y_agregar_dni():
    dni_externos = DNIExterno.query.all()
    print(f'DNI externos encontrados: {len(dni_externos)}')

    for dni_externo in dni_externos:
        print(f'Procesando DNI: {dni_externo.dni}')
        existe = DNIOficial.query.filter_by(dni=dni_externo.dni).first()

        if not existe:
            try:
                print(f'Agregando nuevo DNI: {dni_externo.dni}')
                nuevo_dni = DNIOficial(
                    dni=dni_externo.dni,
                    nombres=dni_externo.nombres,
                    apellidoPaterno=dni_externo.apellidoPaterno,
                    apellidoMaterno=dni_externo.apellidoMaterno,
                    codVerifica=dni_externo.codVerifica,
                    created_at=dni_externo.created_at,
                    updated_at=dni_externo.updated_at
                )
                db.session.add(nuevo_dni)
                db.session.commit()
            except SQLAlchemyError as e:
                print(f'Error al insertar DNI: {e}')
        else:
            print(f'DNI ya existe: {dni_externo.dni}')

# Función para verificar y agregar datos del RUC
def verificar_y_agregar_ruc():
    ruc_externos = RUCExterno.query.all()
    print(f'RUC externos encontrados: {len(ruc_externos)}')

    for ruc_externo in ruc_externos:
        print(f'Procesando RUC: {ruc_externo.ruc}')
        existe = RUCOficial.query.filter_by(ruc=ruc_externo.ruc).first()

        if not existe:
            try:
                print(f'Agregando nuevo RUC: {ruc_externo.ruc}')
                nuevo_ruc = RUCOficial(
                    id=ruc_externo.id,
                    ruc=ruc_externo.ruc,
                    razonSocial=ruc_externo.razonSocial,
                    direccion=ruc_externo.direccion,
                    departamento=ruc_externo.departamento,
                    provincia=ruc_externo.provincia,
                    distrito=ruc_externo.distrito,
                    ubigeo=ruc_externo.ubigeo,
                    estado=ruc_externo.estado,
                    condicion=ruc_externo.condicion,
                    created_at=ruc_externo.created_at,
                    updated_at=ruc_externo.updated_at
                )
                db.session.add(nuevo_ruc)
                db.session.commit()
            except SQLAlchemyError as e:
                print(f'Error al insertar RUC: {e}')
        else:
            print(f'RUC ya existe: {ruc_externo.ruc}')

# Rutas para verificar y agregar los datos
@app.route('/verificar/dni', methods=['POST'])
def verificar_dni():
    verificar_y_agregar_dni()
    return '', 204  # No content

@app.route('/verificar/ruc', methods=['POST'])
def verificar_ruc():
    verificar_y_agregar_ruc()
    return '', 204  # No content

# Ejecutar la aplicación
if __name__ == '__main__':
    app.run(debug=True)
