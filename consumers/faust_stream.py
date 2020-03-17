"""Defines trends calculations for stations"""
import logging

import faust

logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
# TODO: Define the input Kafka Topic. Hint: What topic did Kafka Connect output to?
stations_topic = app.topic("nanodegre.kafka.connect.stations", value_type=Station)

# TODO: Define the output Kafka Topic
out_topic = app.topic("nanodegre.kafka.connect.transformed-stations", partitions=1)

table = app.Table(
    "transformed-stations",
    default=TransformedStation,
    partitions=1,
    changelog_topic=out_topic
)


@app.agent(stations_topic)
async def order(stations):
    async for station in stations:
        line = 'red' if station.red else 'blue' if station.blue else 'green'
        table[station.station_id] = TransformedStation(station_id=station.station_id,
                                                       station_name=station.station_name,
                                                       order=station.order,
                                                       line=line)


if __name__ == "__main__":
    app.main()
