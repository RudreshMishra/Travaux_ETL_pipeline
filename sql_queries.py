# DROP TABLES

event_type_table_drop = "DROP TABLE  IF EXISTS songplays"
service_table_drop = "DROP TABLE IF EXISTS  users"
date_table_drop = "DROP TABLE IF EXISTS  songs"
transaction_table_drop = "DROP TABLE  IF EXISTS artists"

# CREATE TABLES


service_table_create = ("""CREATE TABLE IF NOT EXISTS  services(
	service_id  INT CONSTRAINT service_pk PRIMARY KEY,
	service_name_nl  VARCHAR,
	service_name_en  VARCHAR
)""")


event_type_table_create = ("""CREATE TABLE  IF NOT EXISTS eventtypes(
	event_type_id INT CONSTRAINT eventtype_pk PRIMARY KEY,
	event_name VARCHAR
)""")

date_table_create = ("""CREATE TABLE IF NOT EXISTS date(
	create_time  TIMESTAMP CONSTRAINT time_pk PRIMARY KEY,
	hour INT NOT NULL CHECK (hour >= 0),
	day INT NOT NULL CHECK (day >= 0),
	week INT NOT NULL CHECK (week >= 0),
	month INT NOT NULL CHECK (month >= 0),
	year INT NOT NULL CHECK (year >= 0),
	weekday VARCHAR NOT NULL
)""")


transaction_table_create = ("""CREATE TABLE IF NOT EXISTS eventtransactions(
	event_id SERIAL CONSTRAINT event_pk PRIMARY KEY,
	event_type_id INT REFERENCES eventtypes (event_type_id),
	proffesional_id INT NOT NULL,
	create_time TIMESTAMP REFERENCES date (create_time),
	service_id INT REFERENCES services (service_id),
	cost VARCHAR
)""")

# INSERT RECORDS

event_transaction_table_insert = ("""INSERT INTO eventtransactions VALUES (%s, %s, %s, %s, %s, %s)
""")


# Updating the event type level on conflict
event_type_table_insert = ("""INSERT INTO eventtypes (event_type_id, event_name) VALUES (%s, %s) 

""")

service_table_insert = ("""INSERT INTO services (service_id, service_name_nl, service_name_en) VALUES (%s, %s, %s) 
                        ON CONFLICT (service_id) DO NOTHING                        
""")



time_table_insert = ("""INSERT INTO date VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (create_time) DO NOTHING
""")

# FIND event type

event_type_select = ("""
    SELECT event_type_id
    FROM eventtypes 
    WHERE eventtypes.event_name = %s
""")


# # QUERY LISTS

create_table_queries = [service_table_create, event_type_table_create, date_table_create,transaction_table_create]
drop_table_queries = [event_type_table_drop, service_table_drop, date_table_drop, transaction_table_drop]
