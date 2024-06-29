# Use the official Airflow image as a parent image
FROM apache/airflow:2.9.2

COPY requirements.txt /requirements.txt
RUN pip install -r /requirements.txt

