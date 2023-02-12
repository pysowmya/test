import pandas as pd
import mysql.connector
from mysql.connector import Error
from pandas import DataFrame
from sqlalchemy import create_engine

import os, glob
from pathlib import Path

# Read all the files to merge
read_files = glob.glob("wx_data/*.txt")

# Apply to merge on all the files to generate a single file
dir = 'merged'
filelist = glob.glob(os.path.join(dir, "*"))
for f in filelist:
    os.remove(f)

for f in read_files:
    wx_data_single_file = pd.read_csv(f, delimiter='\t', names=["date", "mintemp", "maxtemp", "precipitation"])
    wx_data_single_file['station_id'] = Path(f).stem
    wx_data_single_file.to_csv('updated/'+Path(f).stem+'_updated.txt', index=False ,columns=["date", "mintemp", "maxtemp", "precipitation", "station_id"])

read_files = glob.glob("updated/*.txt")
with open("merged/result.txt", "wb") as outfile:
    for f in read_files:
        with open(f, "rb") as infile:
            outfile.write(infile.read())

# Read the merged file
wx_data = pd.read_csv('merged/result.txt', delimiter='\t')
print(wx_data.head())

# Read yield data
yld_data = pd.read_csv('yld_data/US_corn_grain_yield.txt', delimiter='\t', header=None)
yld_data.columns = ["year", "count"]
print(wx_data.head())

# Open the connection and create database named demo
try:
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="root12345"
    )
    if conn.is_connected():
        cursor = conn.cursor()
        print("You're connected: ")
        cursor.execute("DROP DATABASE IF EXISTS demo")
        cursor.execute("CREATE DATABASE IF NOT EXISTS demo")
        print("demo database is created")
except Error as e:
    print("Error while connecting to MySQL", e)

# Ingest data into database
try:
    conn = mysql.connector.connect(host='localhost', database='demo', user='root', password='root12345')
    if conn.is_connected():
        cursor = conn.cursor()
        cursor.execute("select database();")
        record = cursor.fetchone()
        print("You're connected to database: ", record)
        print('Creating table....!')
        cursor.execute("CREATE TABLE IF NOT EXISTS weather (date DATE,mintemp INTEGER, maxtemp INTEGER, precipitation INTEGER, station_id VARCHAR(30),PRIMARY KEY(date, mintemp,maxtemp))")
        print("Weather table is created....!")
        for i, row in wx_data.iterrows():
            sql = "INSERT IGNORE INTO demo.weather VALUES ("+row[0].split(',')[0]+","+row[0].split(',')[1]+","+row[0].split(',')[2]+","+row[0].split(',')[3]+", '"+row[0].split(',')[4]+"')"
            cursor.execute(sql)
            conn.commit()

        cursor.execute("UPDATE weather set precipitation=0 where precipitation=-9999;")
        print("Data inserted into the weather table")
except Error as e:
    print("Error while connecting to MySQL", e)

# Retrieve the avg mintemp, max temp and accumulated precipitation group by date
try:
    # Execute query
    sql = "select YEAR(date) as year, avg(mintemp), avg(maxtemp), sum(precipitation) from weather group by YEAR(date);"
    cursor.execute(sql)
    # Fetch all the records
    weather_df = DataFrame(cursor.fetchall(), columns=['year', 'avgmintemp', 'avgmaxtemp', 'accumulateprecipitation'])

except Error as e:
    print("Error while executing the sql cmd", e)

# Store the dataframe in a table named result
try:
    engine = create_engine("mysql://{user}:{pw}@localhost/{db}"
                           .format(user="root",
                                   pw="root12345",
                                   db="demo"))
    weather_df.to_sql('result', con=engine, if_exists='append', chunksize=1000, index=False)
    print("Data stored in the result table")

except Error as e:
    print("Error while executing sql dataframe", e)

# Store yield data
yld_data.to_sql('yield', con=engine, if_exists='append', chunksize=1000, index=False)
print("Yield Data stored in the result table")

# Close the connection
if (conn.is_connected()):
    cursor.close()
    conn.close()
    print("MySQL connection is closed")

