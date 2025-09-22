# ztm\_projekt



Hi and welcome to my repo containing a real-time public transport tracker.

Tech stack used: Kafka, Docker, Pandas, Plotly, Dash



Prerequisites:

* docker desktop installed
* python installed
* python libraries installed: dash, plotly, pandas, gtfs\_realtime\_pb2, kafka-python (if you don't have them, write in your terminal 'pip install dash', etc. for each library. Prerequisite: you have pip installed on your machine)



Instructions:

1. Clone / download the repository to your machine.
2. Go to section GTFS of ZTM Poznań website: https://www.ztm.poznan.pl/otwarte-dane/gtfsfiles/ and download the latest zip file with static data (used as dictionaries)
3. Move the downloaded zip to the folder where you copied my repo to, to subfolder "GTFS-Static"
4. Unzip the folder
5. If for some reason you couldn't download the zip, extract the existing .7z file that is there. It's an example static data from my last manual update - it will work only for as long as I keep updating
6. In the project folder, open 4 terminal windows and write:

* docker-compose up --build
* python producer.py
* python consumer.py
* python dash\_app.py



The last command should provide you with a localhost link that you can open in your browser: http://127.0.0.1:8050/



Thank your interest in my work.

Paweł

