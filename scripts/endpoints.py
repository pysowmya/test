from flask import Flask, json
from pandas import DataFrame
import mysql.connector
from mysql.connector import Error

api = Flask(__name__)


@api.route('/api/weather/stats', methods=['GET'])
def get_weatherinfo():
    try:
        conn = mysql.connector.connect(host='localhost', database='demo', user='root', password='root12345')
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()
            print("You're connected to database: ", record)
        # Execute query
        sql = "select * from result"
        cursor.execute(sql)
        # Fetch all the records
        weather_stats_df = DataFrame(cursor.fetchall(), columns=['year', 'avgmintemp', 'avgmaxtemp', 'accumulateprecipitation'])

    except Error as e:
        print("Error while executing the sql cmd", e)
        # Close the connection

        if conn.is_connected():
            cursor.close()
            conn.close()
            print("MySQL connection is closed")

    result = weather_stats_df.to_json(orient="records")
    parsed = json.loads(result)
    return json.dumps(parsed)


@api.route('/api/weather/<stationId>', methods=['GET'])
def get_weatherdata(stationId):
    try:
        conn = mysql.connector.connect(host='localhost', database='demo', user='root', password='root12345')
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()
            print("You're connected to database: ", record)
        # Execute query
        sql = "select * from weather where station_id='"+stationId+"'"
        cursor.execute(sql)
        # Fetch all the records
        weather_df = DataFrame(cursor.fetchall(), columns=['date', 'mintemp', 'maxtemp', 'precipitation','station_id'])

    except Error as e:
        print("Error while executing the sql cmd", e)
        # Close the connection

        if conn.is_connected():
            cursor.close()
            conn.close()
            print("MySQL connection is closed")

    result = weather_df.to_json(orient="records")
    parsed = json.loads(result)
    return json.dumps(parsed)

@api.route('/api/yield', methods=['GET'])
def get_yielddata():
    try:
        conn = mysql.connector.connect(host='localhost', database='demo', user='root', password='root12345')
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()
            print("You're connected to database: ", record)
        # Execute query
        sql = "select * from yield"
        cursor.execute(sql)
        # Fetch all the records
        yield_df = DataFrame(cursor.fetchall(), columns=['year', 'count'])

    except Error as e:
        print("Error while executing the sql cmd", e)
        # Close the connection

        if conn.is_connected():
            cursor.close()
            conn.close()
            print("MySQL connection is closed")

    result = yield_df.to_json(orient="records")
    parsed = json.loads(result)
    return json.dumps(parsed)

if __name__ == '__main__':
    api.run(host='0.0.0.0', port=5001)
