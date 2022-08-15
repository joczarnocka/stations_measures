from sqlalchemy import Table, Column, Integer, String, Float, Date, MetaData, ForeignKey
from sqlalchemy import create_engine
from sqlalchemy.orm import relationship
import pandas as pd

engine = create_engine('sqlite:///stations.db', echo=True)

meta = MetaData()

stations = Table(
   'stations', meta,
   Column('id', Integer, primary_key=True),
   Column('station', String),
   Column('latitude', Float),
   Column('longitude', Float), 
   Column('elevation', Float),
   Column('name', String),
   Column('country', String),
   Column('state', String)
)


if __name__ == "__main__":

    meta.create_all(engine)
    print(engine.table_names())
    ins = stations.insert()


    df = pd.read_csv("clean_stations.csv", header=0, 
                     names=["station","latitude","longitude","elevation","name","country","state"])
    
    
    ins_records = [] 
    
    for index, row in df.iterrows():        
            record = {
                'station' : row["station"],
                'latitude' : row['latitude'],
                'longitude' : row['longitude'],
                'elevation' : row['elevation'],
                'name' : row['name'],
                'country' : row['country'],
                'state' : row['state']
            }
            ins_records.append(record)

    conn = engine.connect()
    conn.execute(ins, ins_records)

    select1 = stations.select().where(stations.c.id <=5)
    result = conn.execute(select1)

    for row in result:
         print(row)

