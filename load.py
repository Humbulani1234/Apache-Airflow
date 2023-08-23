
import mysql.connector
from new import df_to_tuple


def INSERT_INTO_TABLE(http_address, table_data):
    
    ''' Insert data into an SQL Table function'''
    
    mydb_3 = mysql.connector.connect(host = "127.0.0.1", user = "mysql_user", 
                                     password = "mysql_pass", database = "mysql_db") # Create password ENV variables
    mycursor_3 = mydb_3.cursor()
    result = df_to_tuple(http_address)

    for element in result:
        values = tuple(element)
        mycursor_3.execute(table_data, values)
        mydb_3.commit()
    
    mycursor_3.close()
