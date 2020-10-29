
# Create ETL pipeline on Postgres db

## **Overview**
In this project, we apply Data Modeling with Postgres and build an ETL pipeline using Python. This project is a part of pratice test performed for the enterprise Travaux


## **Event Dataset**
The dataset contains selected events from the professional’s journey on Werkspot between homeowners and proffesionals.


Sample Record :
```
{"event_id": 15, "event_type": "created_account", "professional_id": 15, "created_at": '01/01/2020 21:39:43', "meta_data": ""}
```


## Schema

#### Fact Table 
**eventtransactions** - records in event trasnsaction between the professional and homeowner

```
event_id, event_type_id, proffesional_id, create_time, service_id, cost
```

#### Dimension Tables
**eventtypes**  - different event types
```
event_type_id, event_name
```
**services**  - service offered by homeowner
```
service_id, service_name_nl, service_name_en
```
**date**  - date of event transactions
```
create_time, hour, day, week, month, year, weekday
```


## Project Files

```sql_queries.py``` -> contains sql queries for dropping and  creating fact and dimension tables. Also, contains insertion query template.

```create_tables.py``` -> contains code for setting up database. Running this file creates **travauxdb** and also creates the fact and dimension tables. 

```etl.py``` -> read and process **event_log** 

```test.ipynb``` -> a notebook to connect to postgres db and validate the data loaded.

## Environment 
Python 3.6 or above

PostgresSQL 9.5 or above

psycopg2 - PostgreSQL database adapter for Python


## How to run

Run the drive program ```main.py``` as below to run the question 1 and question 2 direcly
```
python main.py
``` 

To run only question one please run below two script seprately
The ```create_tables.py``` and ```etl.py``` file can also be run independently as below:
```
python create_tables.py 
python etl.py 
```

to run question two please run below two script seprately
```
python availability_snapshot.py
``` 


