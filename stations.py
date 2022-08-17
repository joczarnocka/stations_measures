from sqlalchemy import Table, Column, Integer, String, Float, Date, MetaData, ForeignKey
from sqlalchemy import create_engine
from tables import meta, stations, measures
import pandas as pd
import sqlite3
from sqlite3 import Error


def load_file_to_db(filename, columns, conn, insert_object) -> None:
    """
    load stations from csv file fo DB
    Arguments:
    - filename
    - columns - list of columns
    - engine - database engine
    - insert_object - object representing table
    """
    ins = insert_object.insert()

    df = pd.read_csv(filename, header=0, names=columns)

    dict_records = df.to_dict("records")

    conn.execute(ins, dict_records)


def load_stations(filename, conn, db_object) -> None:
    """
    load stations from csv file fo DB
    Arguments:
    -
    """
    columns = [
        "station",
        "latitude",
        "longitude",
        "elevation",
        "name",
        "country",
        "state",
    ]

    load_file_to_db(filename, columns, conn, db_object)


def load_measures(filename, conn, db_object) -> None:
    """
    load measures from csv file fo DB
    Arguments:
    -filename
    -DB engine
    """
    columns = ["station", "date", "precip", "tobs"]

    load_file_to_db(filename, columns, conn, db_object)


def show_table(select_object, object_name, conn, n) -> None:
    """
    printing first n records of a table 
    Arguments:
    -select_object
    -object_name
    -engine
    -n - number of printed records
    """
    select_tmp = select_object.select().limit(n)
    result = conn.execute(select_tmp)
    print(f"{object_name}:\n===========")
    for row in result:
        print(row)


def show_stations(conn, object_name = stations, n=10) -> None:
    """
    Print first n records of stations
    Arguments:
    -engine - DB engine
    -n - number of records
    """
    show_table(object_name, "Stations", conn, n)


def show_measures(conn, n=20) -> None:
    """
    Print first n records of mesures
    Arguments:
    -engine - DB engine
    -n - number of records
    """
    show_table(measures, "Measures", conn, n)


def check_if_data_loaded(conn, object_select):
    select_tmp = object_select.select().limit(1)
    result = conn.execute(select_tmp)
    return result.first()

def create_connection(db_file):
   """ create a database connection to the SQLite database
       specified by db_file
   :param db_file: database file
   :return: Connection object or None
   """
   conn = None
   try:
       conn = sqlite3.connect(db_file)
       return conn
   except Error as e:
       print(e)

   return conn


if __name__ == "__main__":

    db_file = "stations_measures.db"
    engine = create_engine("sqlite:///" + db_file)  # , echo=True)
    meta.create_all(engine)
    conn = engine.connect()
    print(engine.table_names())
    loaded = check_if_data_loaded(conn, stations)
    if loaded:
        print(
            "Data had been already loaded - the loading will not be repeated (PK restrictions)"
        )
    else:
        print("Data will be loaded")
        load_stations("clean_stations.csv", conn, stations)
        load_measures("clean_measure.csv", conn, measures)
    print("Data:")
    show_table(stations, "Stations", conn, 10)
    show_table(measures, "Measures", conn, 20)
    
    # verification:
    print("\nVerification (SELECT * FROM stations LIMIT 5):\n==============================================")
    dbconn = create_connection(db_file)
    cur = dbconn.cursor()
    rows = cur.execute("SELECT * FROM stations LIMIT 5").fetchall()
    for row in rows:
        print(row)



