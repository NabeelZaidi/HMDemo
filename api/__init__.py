from flask import Flask, render_template, request, redirect,url_for, jsonify
from sqlalchemy import create_engine, event
from urllib.parse import quote_plus
import pyodbc
import urllib
import os

# conn = 'Driver={ODBC Driver 17 for SQL Server};Server=tcp:demoazuresqlhm.database.windows.net,1433;Database=demoazuresqlhmdb;Uid=Nabeel;Pwd=Hanu@1234567;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
# quoted = quote_plus(conn)
# print("********************************")
# print(quoted)
# engine=create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))


# database = 'Driver={ODBC Driver 17 for SQL Server};Server=tcp:demoazuresqlhm.database.windows.net,1433;Database=demoazuresqlhmdb;Uid=Nabeel;Pwd=Hanu@1234567;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
# raw_string = r"{}".format(database)
# print(raw_string)

#params = urllib.parse.quote_plus('Driver={ODBC Driver 17 for SQL Server};Server=tcp:demoazuresqlhm.database.windows.net,1433;Database=demoazuresqlhmdb;Uid=Nabeel;Pwd=Hanu@1234567;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')

#params = urllib.parse.quote_plus(r'Driver={ODBC Driver 17 for SQL Server};Server=tcp:demoazuresqlhm.database.windows.net,1433;Database=demoazuresqlhmdb;Uid=Nabeel;Pwd=Hanu@1234567;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')
# print("First Param")
# print(params)
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
print("Second Param")
print(params)
conn_str = 'mssql+pyodbc:///?odbc_connect={}'.format(params)
print(conn_str)
engine = create_engine(conn_str,echo=True)

# cnxn = pyodbc.connect('Driver={ODBC Driver 13 for SQL Server};Server=tcp:demoazuresqlhm.database.windows.net,1433;Database=demoazuresqlhmdb;Uid=Nabeel;Pwd=Hanu@1234567;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')
# cursor = cnxn.cursor()	
# columns in table x
# print("Getting columns")
# for row in cursor.columns(table='x'):
#     print(row.column_name)

# cursor.execute("SELECT  FROM books") 
# row = cursor.fetchone() 
# while row:
#     print (row) 
#     row = cursor.fetchone()

@event.listens_for(engine, 'before_cursor_execute')
def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
    print("FUNC call")
    if executemany:
        cursor.fast_executemany = True

# print('connection is ok')
#print(engine_azure.table_names())


app = Flask(__name__)



# @app.route('/most_expensive_book')
# def most_expensive_book():
#     query = """SELECT *
#         FROM books 
#         WHERE price = (SELECT max(price) FROM books)"""
#     res = engine.execute(query).fetchall()
#     result = []
#     column_names = engine.execute("PRAGMA table_info(books)").fetchall()
#     for r in res:
#         item = {}
#         for i,col in enumerate(column_names):
#             item[col[1]] = r[i] 
#         result.append(item)
        
#     return jsonify(result)


# @app.route('/categories')
# def categories():
#     query = """SELECT DISTINCT category FROM books"""
    
#     res = engine.execute(query).fetchall()
#     result = {
#         "categories": []
#     }
#     for r in res:
#         result["categories"].append(r[0])
        
#     return jsonify(result)


# @app.route('/categories_count')
# def categories_count():
#     query = """SELECT category, count(*) as number
#                 FROM books
#                 GROUP BY category"""
    
#     res = engine.execute(query).fetchall()
#     result = {
        
#     }
#     for r in res:
#         result[r[0]] = r[1]
        
#     return jsonify(result)


# @app.route('/category/book')
# def category_book():
#     category = request.json.get('category','')
#     query = """SELECT  *
#                 FROM books
#                 WHERE category = ? """
#     res = engine.execute(query,(category,)).fetchall()
#     result = []
#     column_names = engine.execute("PRAGMA table_info(books)").fetchall()
#     for r in res:
#         item = {}
#         for i,col in enumerate(column_names):
#             item[col[1]] = r[i] 
#         result.append(item)
        
#     return jsonify(result)


# @app.route('/book/<string:id>')
# def book(id):
#     query = """SELECT  *
#                 FROM books
#                 WHERE id = ? """
#     r = engine.execute(query,(id,)).fetchone()
#     print("I am printing r")
#     print(r)
    
#     column_names = engine.execute("PRAGMA table_info(books)").fetchall()
#     print("I am printing column names")
#     print(column_names)
#     book = {}
#     for i,col in enumerate(column_names):
#         book[col[1]] = r[i] 
        
#     return jsonify(book)




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