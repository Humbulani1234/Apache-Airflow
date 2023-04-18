#import datetime as dt
import pandas as pd
import requests
from bs4 import BeautifulSoup
from tabulate import tabulate
#import sys
#import time
import mysql.connector
#import os
import warnings

# ============
# Settings
# ============

pd.set_option("display.width", 5000)
pd.set_option("display.max_columns", 500)
pd.set_option("display.max_rows", 600)
warnings.filterwarnings("ignore")
# ===================
# Initializing Inputs
# ===================

http_address = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

#sql_tables = "CREATE TABLE securities (Symbol VARCHAR(255),Security VARCHAR(255),SEC_filings VARCHAR(255),GICS_Sector\
 #VARCHAR(255),GICS_Sub_Industry VARCHAR(255),Headquaters_Location VARCHAR(255),id INT AUTO_INCREMENT PRIMARY KEY)"

#sql_data = "INSERT INTO securities (Symbol, Security, SEC_filings, GICS_Sector, GICS_Sub_Industry, Headquaters_Location\
 #) VALUES (%s,%s,%s,%s,%s,%s)"

#header_data = ["Symbol", "Security", "SEC_filings", "GICS_Sector", "GICS_Sub_Industry", "Headquaters_Location"]

#table_name = "SELECT * FROM securities"

# ========================================
# Extract table and table header functions
# ========================================

def Extract_Table(http_address):
    
    '''Extract table from html page.
    
    Parameters
    ==========
    X_test: Dataframe
            Dataframe for testing model perfomance
    
    ==========
    Returns
    
    predict_binary: Series
                    Predicted series of 0 and 1'''
    
    p = requests.get(http_address)   
    soup = BeautifulSoup(p.content, "html.parser")    
    table = soup.find("table")
    
    return table  
    
#a = Extract_Table(http_address)
#print(a)

def Table_Header(http_address):
    
    '''Extract table header row'''
    
    table = Extract_Table(http_address)    
    f = table.find_all("th")
    
    header = []
    
    for i in f:
        header.append(i.text.strip())
    
    return header   
    
# ===================================
# Extract data (table rows) functions
# ===================================

def Table_Data(http_address):
    
    '''Extract table data (row lines)'''
    
    table = Extract_Table(http_address)
    rows = table.find_all("tr")
    
    data = []
    
    for row in rows:
        td = row.find_all("td")
        
        row_f = []
        
        for i in td:
            row_f.append(i.text.strip())
            
        data.append(row_f)
    data.pop(0)
    
    return data
    
# ========================================
# Create a dictionary/ Dataframe functions
# ========================================

def r(j):
    
    ''' Tool to assist in selecting the relevant item of the data
    to create a dictionary value for each key (table header)'''
    
    path_data = []
    data = Table_Data(http_address)
    
    for item in data:
        path_data.append(item[j])
        
    return path_data


def Dataframe(http_address):
    
    '''Create a dictionary of key(table header) and value(data)
    and convert to dictionary for readability'''
    
    data = Table_Data(http_address)     

    final_data = []

    d = [*range(len(data[0]))]
    path_data = map(r, d)
    dictionary = dict(zip(Table_Header(http_address), list(path_data)))    
    df = pd.DataFrame.from_dict(dictionary)
    df.drop(labels=["Date added", "Founded"], axis=1, inplace=True)
    
    return df

#a = Dataframe(http_address)
#print(a)

def df_to_tuple(http_address):
    
    '''Convert dataframe rows to tuple for ease of readility 
    when inserting data into SQL table'''

    records = Dataframe(http_address).to_records(index=False)
    result = list(records)
                        
    return result

#a = df_to_tuple(http_address)
#print(a)

# ===============================
# Create an SQL database function
# ===============================

#def CREATE_DATABASE():
    
    #''' Create and SQL database function'''
    #cursor.execute("DROP DATABASE IF EXISTS roads")
    #mydb = mysql.connector.connect(host = "localhost", user = "mysql_user", password = "mysql_pass")
    #mycursor = mydb.cursor()
    #mycursor.execute("DROP DATABASE IF EXISTS mysql_db")
    #mycursor.execute("CREATE DATABASE mysql_db") # Create database
    #mycursor.execute("SHOW DATABASES") # Show available databases

    #for x in mycursor: 
        #print(x)
        
    #mycursor.close()
        
#CREATE_DATABASE()

# ============================
# Create an SQL Table function
#=============================

#def CREATE_TABLES(data):
    
    #''' Create an SQL Table function'''
    
    #mydb_2 = mysql.connector.connect(host = "localhost", user = "mysql_user", password = "mysql_pass", database = "mysql_db")
    #mycursor_2 = mydb_2.cursor()
    #mycursor_2.execute("DROP TABLE IF EXISTS securities")
    #mycursor_2.execute(sql_tables)
    #mycursor_2.execute("SHOW TABLES")

    #for x in mycursor_2: 
        #print(x)
    
    #mycursor_2.close()
    
#CREATE_TABLES(data=sql_tables)

# =======================================
# Insert data into and SQL Table function
#========================================

#def INSERT_INTO_TABLE(http_address, table_data):
    
    #''' Insert data into an SQL Table function'''
    
    #mydb_3 = mysql.connector.connect(host = "127.0.0.1", user = "mysql_user", password = "mysql_pass", database = "mysql_db")
    #mycursor_3 = mydb_3.cursor()
    #result = df_to_tuple(http_address)

    #for element in result:
        #values = tuple(element)
        #mycursor_3.execute(table_data, values)
        #mydb_3.commit()
    
    #mycursor_3.execute("SHOW TABLES")
    
    #for x in mycursor_3:
        #print(x)
    
    #print(mycursor_3.rowcount, "was inserted")
    
    #mycursor_3.close()
    
#INSERT_INTO_TABLE(http_address, sql_data)

# =============================
# Print and SQL Table in python
# =============================

#def Get_sql_table(header_data, table_name):
    
    #'''Print and SQL Table in python'''
    
    #mydb_4 = mysql.connector.connect(host = "localhost", user = "mysql_user", password = "mysql_pass", database = "mysql_db")
    #mycursor_4 = mydb_4.cursor()
    #mycursor_4.execute(table_name)
    #my_result = mycursor_4.fetchall()

    #return tabulate(my_result, headers = header_data, tablefmt="psql")
    
    #mycursor_4.close()      

# ===============
# Run the script
# ===============

#print(Get_sql_table(header_data, table_name))

# ==================
# Total process time
# ==================

#print("Process time in seconds: " + str((time.time() - start)))

# ====================================================================================================================================




