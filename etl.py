import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
from datetime import datetime
import numpy as np

def process_log_file(cur, filepath):
    """
    Process Event log files and insert records into the Postgres database.
    :param cur: cursor reference
    :param filepath: complete file path for the file to load
    """
    # open log file
    df = df = pd.read_csv(filepath)

    # convert timestamp column to datetime
    t = pd.to_datetime(df['created_at'])
    # insert date data records
    column_labels = ["timestamp", "hour", "day", "weelofyear", "month", "year", "weekday"]
    time_data = []
    for data in t:
        time_data.append([data ,data.hour, data.day, data.weekofyear, data.month, data.year, data.day_name()])

    time_df = pd.DataFrame.from_records(data = time_data, columns = column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load event type
    user_df = df.event_type.unique()
    # insert event type records
    for i,row in enumerate(user_df):
        cur.execute(event_type_table_insert,[i,row])

    # filter by proposed action to process service data
    df1 = df[df['event_type'] == "proposed"]
    service_metadatas = df1[['event_id','meta_data']]
    service_data = []
    event_service_data = []
    for index, service_metadata in service_metadatas.iterrows():
        service_id, service_name_nl, service_name_en, cost = service_metadata.meta_data.strip().split('_')
        service_data.append([service_id, service_name_nl, service_name_en])
        event_service_data.append([service_metadata.event_id, service_id, cost])

    column_labels = ["service_id", "service_name_nl", "service_name_en"]
    service_df = pd.DataFrame.from_records(data = service_data, columns = column_labels)

    for i, row in service_df.iterrows():
        cur.execute(service_table_insert, list(row))

    column_labels = ["event_id", "service_id", "cost"]
    event_service_df = pd.DataFrame.from_records(data = event_service_data, columns = column_labels)


    # insert event records records
    for index, row in df.iterrows():
        
        # get eventype from event type tables
        cur.execute(event_type_select, [row.event_type])
        results = cur.fetchone()
        
        if not pd.isna(row.meta_data):
            event_service = event_service_df[event_service_df.event_id==row.event_id]
            event_service=  event_service.values
            service_id , cost = event_service[0][1], str(event_service[0][2])
        else:
            service_id , cost = None, None
        # insert event transaction record
        event_transaction_data = ( row.event_id, results, row.professional_id_anonymized, row.created_at,service_id, cost)
        cur.execute(event_transaction_table_insert, event_transaction_data)

def process_data(cur, conn, filepath, func):
    """
    Driver function to load data from event log files into Postgres database.
    :param cur: a database cursor reference
    :param conn: database connection reference
    :param filepath: parent directory where the files exists
    :param func: function to call
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.csv'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    Driver function for loading event facts and dimensions data into Postgres database
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=travauxdb user=postgres password=postgress")
    cur = conn.cursor()
    process_data(cur, conn, filepath='data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
    print("\n\nFinished processing!!!\n\n")
