import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import time
from datetime import datetime
import pytz

def download_gtfs_rt_file(url: str) -> bytes:
    response = requests.get(url)
    response.raise_for_status()
    return response.content

kafka_broker = "localhost:9092"

while True:
    try:
        producer = KafkaProducer(bootstrap_servers=kafka_broker)
        print("Połączono z Kafka")
        break
    except NoBrokersAvailable:
        print("Kafka broker niedostępny, czekam 5 sekund...")
        time.sleep(5)

vehicle_positions_url = "https://www.ztm.poznan.pl/pl/dla-deweloperow/getGtfsRtFile?file=vehicle_positions.pb"
filename = "vehicle_positions.pb"

last_data = None
cet = pytz.timezone("Europe/Warsaw")

while True:
    try:
        data = download_gtfs_rt_file(vehicle_positions_url)
        now = datetime.now(tz=cet).strftime("%H:%M:%S CET")

        # Sprawdzenie czy pobrany plik nie jest pusty
        if not data:
            print(f"[{now}] Otrzymano pusty plik, pomijam wysyłkę")
            time.sleep(5)
            continue

        if last_data is None or data != last_data:
            producer.send(
                'gtfs_vehicle_positions',
                value=data,
                headers=[("filename", filename.encode("utf-8"))]
            )
            producer.flush()
            print(f"[{now}] Wysłano {filename} do Kafka - plik się zmienił")
            last_data = data
        else:
            print(f"[{now}] Plik .pb niezmieniony - pomijam wysyłkę")
    except Exception as e:
        now = datetime.now(tz=cet).strftime("%H:%M:%S CET")
        print(f"[{now}] Błąd pobierania lub wysyłki pliku: {e}")
    time.sleep(2)
