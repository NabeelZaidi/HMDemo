from flask import Flask, render_template, request, redirect,url_for, jsonify
from sqlalchemy import create_engine, event
from urllib.parse import quote_plus
import pyodbc
import urllib
import os
#Connecting with Database
driver = "{ODBC Driver 17 for SQL Server}"
server = "demoazuresqlhm.database.windows.net"
database = "demoazuresqlhmdb"
username = "Nabeel"
password = os.environ['DatabaseConnectionStringPassword']
params = urllib.parse.quote_plus(
    'Driver=%s;' % driver +
    'Server=tcp:%s,1433;' % server +
    'Database=%s;' % database +
    'Uid=%s;' % username +
    'Pwd=%s;' % password +
    'Encrypt=yes;' +
    'TrustServerCertificate=no;' +
    'Connection Timeout=30;')


conn_str = 'mssql+pyodbc:///?odbc_connect={}'.format(params)

engine = create_engine(conn_str,echo=True)


@event.listens_for(engine, 'before_cursor_execute')
def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
    print("FUNC call")
    if executemany:
        cursor.fast_executemany = True


#print(engine_azure.table_names())


app = Flask(__name__)


#Which is the most expensive book? If there are multiple books which share themax() price return all of them
@app.route('/most_expensive_book')
def most_expensive_book():
    query = """SELECT id, title, price, stock, category
        FROM books 
        WHERE price = (SELECT max(price) FROM books)"""
    res = engine.execute(query).fetchall()
    result = []
    
    for r in res:
        result.append({
            "id" : r[0],
            "title" : r[1],
            "price" : r[2],
            "stock" : r[3],
            "category" : r[4]
        })
        
    return jsonify(result)

#List scraped book categories
@app.route('/categories')
def categories():
    query = """SELECT DISTINCT category FROM books"""
    
    res = engine.execute(query).fetchall()
    result = {
        "categories": []
    }
    for r in res:
        result["categories"].append(r[0])
        
    return jsonify(result)

#How many books have been scraped per category?
@app.route('/categories_count')
def categories_count():
    query = """SELECT category, count(*) as number
                FROM books
                GROUP BY category"""
    
    res = engine.execute(query).fetchall()
    result = {
        
    }
    for r in res:
        result[r[0]] = r[1]
        
    return jsonify(result)

#List all of the books for a category
@app.route('/category/book')
def category_book():
    category = request.json.get('category','')
    query = """SELECT id, title, price, stock, category
                FROM books
                WHERE category = ? """
    res = engine.execute(query,(category,)).fetchall()
    result = []
    for r in res:
        result.append({
            "id" : r[0],
            "title" : r[1],
            "price" : r[2],
            "stock" : r[3],
            "category" : r[4]
        })
        
    return jsonify(result)

#Return a single book by ID (you can create your own ID or take it from the scraped data)
@app.route('/book/<string:id>')
def book(id):
    query = """SELECT id, title, price, stock, category
                FROM books
                WHERE id = ? """
    r = engine.execute(query,(id,)).fetchone()
    book = {
        "id" : r[0],
        "title" : r[1],
        "price" : r[2],
        "stock" : r[3],
        "category" : r[4]
    }
    
        
    return jsonify(book)
