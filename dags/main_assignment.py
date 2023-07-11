import datetime
import json
from dataclasses import dataclass
from datetime import datetime

from airflow.decorators import dag, task


import requests


@dag(
    schedule=None,
    start_date=datetime(2023, 1, 11),
    catchup=False
)
def main_assignment():

    @task
    def get_departed_flights_from_cdg():
        url = 'https://opensky-network.org/api/flights/departure'
        params = {
            'airport': 'EDDF',
            'begin': 1688478878,
            'end': 1688565278
        }

        response = requests.get(url, params=params)
        if response.status_code == 200:
            print(response.json())
            return response.json()
        else:
            print('Error:', response.status_code)
            return None

    @task
    def write_flights_to_json(flights_text):
        filename='./dags/main_flights.json'
        try:
            print("Flight data received:")
            print(flights_text)
            with open(filename, 'w') as file:
                json.dump(flights_text, file, indent=4)
            print(f"Flight data written to '{filename}' successfully.")
        except Exception as e:
            print('Error:', str(e))

    response_text = get_departed_flights_from_cdg()
    if response_text:
        write_flights_to_json(response_text)

main_assignment = main_assignment()