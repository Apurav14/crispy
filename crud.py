from flask import Flask, request, jsonify
from flask_pymongo import PyMongo
from bson.json_util import dumps
from marshmallow import Schema, fields, ValidationError, validates, validate
import configparser
import logging

logging.basicConfig(level=logging.INFO)

config = configparser.ConfigParser()
config.read("config.cnf")
logging.info('Fetching Configuration Details...')
password = config.get('details', 'password')
hostname = config.get('details', 'host')

app = Flask(__name__)

app.secret_key = password
app.config['MONGO_URI'] = hostname

# print(hostname)

mongo = PyMongo(app)
logging.info('Connected to MongoDB')


# Create Operation
@app.route('/add', methods=['POST'])
def add_user():
    _json = request.json
    _name = _json['name']
    _age = _json['age']
    _role = _json['role']

    if _name and _age and _role and request.method == 'POST':
        logging.info('Checking the values entered...')
        a = validator(_name, _age, _role)
        print(a)
        mongo.db.employees.insert_one(
            {'name': _name, 'age': _age, 'role': _role}
            )
        resp = jsonify("User Added")
        resp.status_code = 200
        logging.info('User added!')
        return resp
    else:
        logging.error('Kindly check the details. Fields are: name, age, role.')
        return not_found()


# Read Operation

@app.route('/users')
def users():
    users = mongo.db.employees.find()
    resp = dumps(users)
    logging.info('Getting list of users...')
    return resp


# Update Operation

@app.route('/update/<name>', methods=['PUT'])
def update_user(name):
    _json = request.json
    _name = _json['name']
    _age = _json['age']
    _role = _json['role']

    if _name and _role and _age and request.method == 'PUT':
        logging.info('Checking the values to be updated...')
        b = validator(_name, _age, _role)
        print(b)
        mongo.db.employees.update_one(
            {'name': name},
            {'$set': {"name": _name, 'age': _age, 'role': _role}}
            )
        resp = jsonify("User updated successfully")
        logging.info('User Updated!')
        resp.status_code = 200
        return resp
    else:
        logging.error('Kindly check the fields to be updated')
        return not_found()


# Delete Operation

@app.route('/delete/<name>', methods=['DELETE'])
def delete_user(name):
    mongo.db.employees.delete_one({'name': name})
    resp = jsonify("User named {} is Deleted Successfully".format(name))
    logging.info('User Deleted!')
    resp.status_code = 200
    return resp


# Error Handling

@app.errorhandler(404)
def not_found(error=None):
    message = {
        'status': 404,
        'message': 'Not Found. Use /users to read the database.'
    }
    logging.error('Kindly check the URL again.')
    resp = jsonify(message)
    resp.status_code = 404
    return resp


# Serialization & Deserialization

def validator(n, a, r):
    input_data = {}
    input_data['name'] = n
    input_data['age'] = a
    input_data['role'] = r
    schema = PersonSchema()
    user = schema.load(input_data)
    result = schema.dump(user)
    return result


class PersonSchema(Schema):
    name = fields.String(validate=validate.Length(max=5))
    age = fields.Integer()
    role = fields.String()

    @validates('age')
    def validate_age(self, age):
        if age > 100 or age <= 0:
            raise ValidationError('Please enter a Valid age.')


if __name__ == "__main__":
    logging.info('Started.')
    app.run(debug=True)
