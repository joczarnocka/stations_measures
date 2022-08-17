from sqlalchemy import Table, Column, Integer, String, Float, Date, MetaData, ForeignKey
from sqlalchemy import create_engine
from sqlalchemy.orm import relationship
import pandas as pd

meta = MetaData()

stations = Table(
    "stations",
    meta,
    Column("station", String, primary_key=True),
    Column("latitude", Float),
    Column("longitude", Float),
    Column("elevation", Float),
    Column("name", String),
    Column("country", String),
    Column("state", String),
)

measures = Table(
    "measures",
    meta,
    Column("id", Integer, primary_key=True),
    Column("station", String, ForeignKey("stations.station"), nullable=False),
    Column("date", String),
    Column("precip", Float),
    Column("tobs", Integer),
)


def load_file_to_db(filename, columns, engine, insert_object) -> None:
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

    conn = engine.connect()
    conn.execute(ins, dict_records)


def load_stations(filename, engine,) -> None:
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

    load_file_to_db(filename, columns, engine, stations)


def load_measures(filename, engine) -> None:
    """
    load measures from csv file fo DB
    Arguments:
    -filename
    -DB engine
    """
    columns = ["station", "date", "precip", "tobs"]

    load_file_to_db(filename, columns, engine, measures)


def show_table(select_object, object_name, engine, n) -> None:
    """
    printing first n records of a table 
    Arguments:
    -select_object
    -object_name
    -engine
    -n - number of printed records
    """
    conn = engine.connect()
    select_tmp = select_object.select().limit(n)
    result = conn.execute(select_tmp)
    print(f"{object_name}:\n===========")
    for row in result:
        print(row)


def show_stations(engine, n=10) -> None:
    """
    Print first n records of stations
    Arguments:
    -engine - DB engine
    -n - number of records
    """
    show_table(stations, "Stations", engine, n)


def show_measures(enginem, n=20) -> None:
    """
    Print first n records of mesures
    Arguments:
    -engine - DB engine
    -n - number of records
    """
    show_table(measures, "Measures", engine, n)


def check_if_data_loaded(engine):
    conn = engine.connect()
    select_tmp = stations.select().limit(1)
    result = conn.execute(select_tmp)
    for _ in result:
        return True
    return False


if __name__ == "__main__":
    engine = create_engine("sqlite:///stations_measures.db")  # , echo=True)
    meta.create_all(engine)
    print(engine.table_names())
    loaded = check_if_data_loaded(engine)
    if loaded:
        print(
            "Data had been already loaded - the loading will not be repeated (PK restrictions)"
        )
    else:
        print("Data will be loaded")
        load_stations("clean_stations.csv", engine)
        load_measures("clean_measure.csv", engine)
    print("Data:")
    show_stations(engine)
    show_measures(engine)
