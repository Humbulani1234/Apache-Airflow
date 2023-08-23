

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tabulate import tabulate
import mysql.connector
import warning

pd.set_option("display.width", 5000)
pd.set_option("display.max_columns", 500)
pd.set_option("display.max_rows", 600)
warnings.filterwarnings("ignore")

http_address = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

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

def Table_Header(http_address):
    
    '''Extract table header row'''
    
    table = Extract_Table(http_address)    
    f = table.find_all("th")
    
    header = []
    
    for i in f:
        header.append(i.text.strip())
    
    return header   

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

def df_to_tuple(http_address):
    
    '''Convert dataframe rows to tuple for ease of readility 
    when inserting data into SQL table'''

    records = Dataframe(http_address).to_records(index=False)
    result = list(records)
                        
    return result