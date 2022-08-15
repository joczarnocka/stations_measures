from sqlalchemy import Table, Column, Integer, String, Float, Date, MetaData, ForeignKey
from sqlalchemy import create_engine
from sqlalchemy.orm import relationship
import pandas as pd

engine = create_engine("sqlite:///stations_v2.db", echo=True)

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
    Column("date", Date),
    Column("precip", Float),
    Column("tobs", Integer),
)


if __name__ == "__main__":

    meta.create_all(engine)
    print(engine.table_names())

    # load stations:
    ins = stations.insert()
    df = pd.read_csv(
        "clean_stations.csv",
        header=0,
        names=[
            "station",
            "latitude",
            "longitude",
            "elevation",
            "name",
            "country",
            "state",
        ],
    )

    ins_records = []

    for index, row in df.iterrows():
        record = {
            "station": row["station"],
            "latitude": row["latitude"],
            "longitude": row["longitude"],
            "elevation": row["elevation"],
            "name": row["name"],
            "country": row["country"],
            "state": row["state"],
        }
        ins_records.append(record)

    conn = engine.connect()
    conn.execute(ins, ins_records)

    select1 = stations.select()
    result = conn.execute(select1)

    print("Stations:\n==============")
    for row in result:
        print(row)

    # load measures:
    ins_measures = measures.insert()
    df = pd.read_csv(
        "clean_measures.csv", header=0, names=["station", "date", "precip", "tobs"]
    )

    ins_records = []

    for index, row in df.iterrows():
        record = {
            "station": row["station"],
            "date": row["date"],
            "precip": row["precip"],
            "tobs": row["tobs"],
        }
        ins_records.append(record)

    conn = engine.connect()
    conn.execute(ins_measures, ins_records)

    select2 = measures.select()
    result = conn.execute(select2)

    print("Measures:\n==============")
    for row in result:
        print(row)
