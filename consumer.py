from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import time
from datetime import datetime
import pytz
import os

kafka_broker = "localhost:9092"
topic_name = "gtfs_vehicle_positions"
output_dir = "./received_files"
cet = pytz.timezone("Europe/Warsaw")

os.makedirs(output_dir, exist_ok=True)

while True:
    try:
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=kafka_broker,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            consumer_timeout_ms=1000
        )
        print("Połączono z Kafka Consumer")
        break
    except NoBrokersAvailable:
        print("Kafka broker niedostępny, czekam 5 sekund...")
        time.sleep(5)

print("Nasłuchiwanie nowości na topicu:", topic_name)

def atomic_save_file(data: bytes, final_path: str):
    tmp_path = final_path + ".tmp"
    with open(tmp_path, "wb") as f:
        f.write(data)
    os.replace(tmp_path, final_path)
    print(f"Zapisano plik: {final_path}")

while True:
    try:
        for msg in consumer:
            now = datetime.now(tz=cet).strftime("%H:%M:%S CET")
            headers = dict((k, v.decode()) for k, v in msg.headers) if msg.headers else {}
            filename = headers.get("filename", f"file_{msg.offset}.pb")
            final_filepath = os.path.join(output_dir, filename)
            atomic_save_file(msg.value, final_filepath)
            print(f"[{now}] Odebrano i zapisano plik {filename}")
        time.sleep(2)  # odpytuj co 2 sekundy, niezależnie od aktualnej sekundy
    except Exception as e:
        now = datetime.now(tz=cet).strftime("%H:%M:%S CET")
        print(f"[{now}] Błąd konsumowania wiadomości: {e}")
        time.sleep(5)
