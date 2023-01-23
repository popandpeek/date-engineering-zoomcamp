FROM python:3.9.1

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2

WORKDIR /app
COPY upload-data.py upload-data.py
COPY green_tripdata_2019-01.csv green_tripdata_2019-01.csv
COPY taxi+_zone_lookup.csv taxi+_zone_lookup.csv

ENTRYPOINT [ "python", "upload-data.py" ]