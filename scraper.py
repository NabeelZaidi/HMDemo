from bs4 import BeautifulSoup
import pyodbc
import requests
import pandas as pd
from sqlalchemy import create_engine, event
from urllib.parse import quote_plus
import urllib
import re


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


@event.listens_for(engine, 'before_cursor_execute')
def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
    print("FUNC call")
    if executemany:
        cursor.fast_executemany = True

# creating the lists for names and prices
names = []
prices= []
ids = []
stock = []
categories = []

# Define the range of pages to work
pages = [str(i) for i in range(1,15)]  

for page in pages:
    # using request to get the HTML data
    response = requests.get('http://books.toscrape.com/catalogue/page-' + page + '.html')

    # creating the soup, that will read the document and provide access to the information
    soup = BeautifulSoup(response.text, 'html.parser')

    # now our results will include information about each book
    results = soup.find_all(attrs={'class':'product_pod'})

    print(f"page {page}/{len(pages)}")
    # let's check how many results we got
    for i,book in enumerate(results):
        title = book.h3.a.get('title')
        href = book.h3.a.get('href')

        names.append(title)
        prices.append(float(book.find('p', class_="price_color").text[2:]) )

        book_response = requests.get('http://books.toscrape.com/catalogue/' + href )
        soup2 = BeautifulSoup(book_response.text, 'html.parser')
        rows = soup2.find('table').find_all('tr')
        for i,row in enumerate(rows):
            cols=row.find_all('td')
            cols=[x.text.strip() for x in cols]
            if i == 0:
                ids.append(cols[0])
            elif i == 5:
                quantity = re.findall(r"\(([0-9]*).*",cols[0])[0]
                stock.append(int(quantity))
        
        category = soup2.find('ul').find_all('li')[2].a.text
        categories.append(category)
        

# creating the data frame with the lists
books_df= pd.DataFrame(list(zip(ids,names, prices,stock,categories)), columns=['id','title','price','stock','category'])
books_df.to_sql("books",con=engine,if_exists="replace")
# print(books_df.head())
