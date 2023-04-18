
#sql_tables = "CREATE TABLE securities (Symbol VARCHAR(255),Security VARCHAR(255>
 #VARCHAR(255),GICS_Sub_Industry VARCHAR(255),Headquaters_Location VARCHAR(255),>

#sql_data = "INSERT INTO securities (Symbol, Security, SEC_filings, GICS_Sector,>
 #) VALUES (%s,%s,%s,%s,%s,%s)"

#header_data = ["Symbol", "Security", "SEC_filings", "GICS_Sector", "GICS_Sub_In>

#table_name = "SELECT * FROM securities"

import mysql.connector
#import new
from new import df_to_tuple

# =======================================
# Insert data into and SQL Table function
#========================================

def INSERT_INTO_TABLE(http_address, table_data):
    
    ''' Insert data into an SQL Table function'''
    
    mydb_3 = mysql.connector.connect(host = "127.0.0.1", user = "mysql_user", password = "mysql_pass", database = "mysql_db")
    mycursor_3 = mydb_3.cursor()
    result = df_to_tuple(http_address)

    for element in result:
        values = tuple(element)
        mycursor_3.execute(table_data, values)
        mydb_3.commit()
    
    #mycursor_3.execute("SHOW TABLES")
    
    #for x in mycursor_3:
        #print(x)
    
    #print(mycursor_3.rowcount, "was inserted")
    
    mycursor_3.close()
    
#INSERT_INTO_TABLE(http_address, sql_data)
