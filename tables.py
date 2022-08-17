from sqlalchemy import Table, Column, Integer, String, Float, Date, MetaData, ForeignKey

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