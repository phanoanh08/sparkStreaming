from pymongo import MongoClient
import sys
import mysql.connector

def connectDatabase(db_name):
    connection_string = 'mongodb+srv://oanh:123456a_@cluster0.fqwqeau.mongodb.net/?retryWrites=true&w=majority'
    client = MongoClient(connection_string)
    try:
        print('Successful connection!')
    except Exception:
        print("Unable to connect to the server!")
    return client[db_name]

def mysqlConnector(username, pw, db):
    my_db = mysql.connector.connect(
        host="localhost",
        user=username, 
        password=pw,
        database=db
    )
    return my_db 