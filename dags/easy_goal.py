import datetime
import json
from dataclasses import dataclass
from datetime import datetime

from airflow import DAG
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

import requests

class FlightData:
    def __init__(self, icao24, first_seen, est_departure_airport, last_seen,
                 est_arrival_airport, callsign, est_departure_airport_horiz_distance,
                 est_departure_airport_vert_distance, est_arrival_airport_horiz_distance,
                 est_arrival_airport_vert_distance, departure_airport_candidates_count,
                 arrival_airport_candidates_count):
        self.icao24 = icao24
        self.first_seen = first_seen
        self.est_departure_airport = est_departure_airport
        self.last_seen = last_seen
        self.est_arrival_airport = est_arrival_airport
        self.callsign = callsign
        self.est_departure_airport_horiz_distance = est_departure_airport_horiz_distance
        self.est_departure_airport_vert_distance = est_departure_airport_vert_distance
        self.est_arrival_airport_horiz_distance = est_arrival_airport_horiz_distance
        self.est_arrival_airport_vert_distance = est_arrival_airport_vert_distance
        self.departure_airport_candidates_count = departure_airport_candidates_count
        self.arrival_airport_candidates_count = arrival_airport_candidates_count

    @classmethod
    def from_json(cls, json_data):
        return cls(
            icao24=json_data['icao24'],
            first_seen=json_data['firstSeen'],
            est_departure_airport=json_data['estDepartureAirport'],
            last_seen=json_data['lastSeen'],
            est_arrival_airport=json_data['estArrivalAirport'],
            callsign=json_data['callsign'],
            est_departure_airport_horiz_distance=json_data['estDepartureAirportHorizDistance'],
            est_departure_airport_vert_distance=json_data['estDepartureAirportVertDistance'],
            est_arrival_airport_horiz_distance=json_data['estArrivalAirportHorizDistance'],
            est_arrival_airport_vert_distance=json_data['estArrivalAirportVertDistance'],
            departure_airport_candidates_count=json_data['departureAirportCandidatesCount'],
            arrival_airport_candidates_count=json_data['arrivalAirportCandidatesCount']
        )



@dag(
    schedule_interval=None,
    start_date=datetime(2023, 1, 11),
    catchup=False,
)
def easy_assignment():
    
    @task(multiple_outputs=True)
    def get_departed_flights_from_cdg():
        url = 'https://opensky-network.org/api/flights/departure'
        params = {
            'airport': 'EDDF',
            'begin': 1688478878,
            'end': 1688565278
        }

        response = requests.get(url, params=params)
        departures = []
        if response.status_code == 200:
            json_departures = response.json()
            for i, json_departure in enumerate(json_departures):
                departure = FlightData.from_json(json_departure)
                departures.append(departure)

            serialized_departures = json.dumps([d.__dict__ for d in departures])
            return {
                'departures_list': serialized_departures
            }

        else:
            print('Error:', response.status_code)
            return None
    
    @task
    def write_flights_to_json(flights_text):
        filename='./dags/easy_flights.json'
        departures= json.loads(flights_text)
        try:
            print("Flight data received:")
            print(departures)
            with open(filename, 'w') as file:
                json.dump(departures, file, indent=4)
            print(f"Flight data written to '{filename}' successfully.")
        except Exception as e:
            print('Error:', str(e))

    
    departures_output = get_departed_flights_from_cdg()

    print('00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000')

    if departures_output:
        departures = departures_output['departures_list']
        write_flights_to_json(departures)


easy_assignment = easy_assignment()
