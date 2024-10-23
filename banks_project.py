# Code for ETL operations on Country-GDP data

# Importing the required libraries

import requests as rq
from bs4 import BeautifulSoup as soup
import pandas as pd
import sqlite3 as sql
import numpy as np
from datetime import datetime

url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ["Name", "MC_USD_Billion"]
db_name = 'Banks.db'
table_name = 'Largest_banks'
output_path = './Largest_banks_data.csv'


def log_progress(message):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_entry = f"{timestamp} : {message}\n"

    with open('code_log.txt', 'a') as log_file:
        log_file.write(log_entry)

# Assuming this is where you start your script
log_progress("Preliminaries complete. Initiating ETL process")

def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    website = rq.get(url).text
    data = soup(website,'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    list = [] # List to hold row data
    for row in rows[1:]:  # Skip the header row
        cols = row.find_all('td')
        if len(cols) !=0:
            name = cols[1].text.strip()
            mc_usd_billion = cols[2].text.strip()
            list.append({"Name": name, "MC_USD_Billion": mc_usd_billion})
    new_data_df = pd.DataFrame(list) # Convert list to DataFrame and concatenate with the empty DataFrame
    df = pd.concat([df, new_data_df], ignore_index=True)
    return df

def transform(df):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    exchange_rate = pd.read_csv("exchange_rate.csv")
    dict = exchange_rate.set_index('Currency').to_dict()['Rate']
    df['MC_USD_Billion'] = df['MC_USD_Billion'].astype(float)
    df['MC_GBP_Billion'] = [np.round(x * dict['GBP'], 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * dict['EUR'], 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * dict['INR'], 2) for x in df['MC_USD_Billion']]
    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name,sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

log_progress('Preliminaries complete. Initiating ETL process')

# Extract data
df = extract(url, table_attribs)
log_progress('Data extraction complete. Initiating Transformation process')
# Transform data
df = transform(df)
log_progress('Data transformation complete. Initiating loading process')
# Load to CSV
load_to_csv(df, output_path)
log_progress('Data saved to CSV file')
# Establish SQLite connection
sql_connection = sql.connect(db_name)
log_progress('SQL Connection initiated.')
# Load to SQLite database
load_to_db(df, sql_connection, table_name)
log_progress('Data loaded to Database as table. Running the query')

# Display Result

query_statement = f"SELECT * FROM Largest_banks"
run_query(query_statement, sql_connection)
log_progress('SQL Query')
query_statement = f"SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
run_query(query_statement, sql_connection)
log_progress('SQL Query')
query_statement = f"SELECT Name from Largest_banks LIMIT 5"
run_query(query_statement, sql_connection)
log_progress('SQL Query')

# pd.set_option('display.max_columns', None) # display all columns
# print(df)

sql_connection.close()
log_progress('SQL Connection Close')



# ''' Here, you define the required entities and call the relevant
# functions in the correct order to complete the project. Note that this
# portion is not inside any function.'''

