import psycopg2
import pandas as pd
from datetime import datetime
import numpy as np

def connect_db():
    """
    Establishes database connection and return's the connection and cursor references.
    :return: return's (cur, conn) a cursor and connection reference
    """
    # connect to default database

    conn = psycopg2.connect("host=127.0.0.1 dbname=travauxdb user=postgres password=postgres")
    cur = conn.cursor()
    return cur, conn


def extract_data(conn):
    """
    Run's all the drop table queries defined in sql_queries.py
    :param conn: database connection reference
    """

    # sql statment in order to extract the data on a specific day for each professional id.

    sql_query = pd.read_sql_query(
    '''SELECT proffesional_id
        , create_time
        , event_type_id
    FROM (
        SELECT RANK() OVER (PARTITION BY proffesional_id ORDER BY create_time) AS RN
            , proffesional_id
            , create_time
            ,event_type_id
        FROM eventtransactions where event_type_id in (1,2) and DATE(create_time) between (select min(create_time) from eventtransactions) and '2020/03/10' ORDER BY proffesional_id
        ) AS ST
    WHERE ST.RN = 1;''', conn)
    df = pd.DataFrame(sql_query, columns=['proffesional_id','create_time','event_type_id'])

    return df


def update_data(df1, startdate, enddate):
    """
    Run's  in order to update dataframe betwen given range of date
    :param df1: pandas dataframe for availability snapshot
    :param startdate: startdate when the professional is active
    :param enddate: end date when the professional is active
    """

    mask = (df1['create_time'] >= startdate) & (df1['create_time'] <= enddate)
    df1.loc[mask, ['Active_professionals_count']] += 1


def process_data(df):
    """
    Run's  in order to process dataframe that would count the amount of active professionals per day
    :param df1: pandas dataframe for availability snapshot
    """

    df['create_time'] = df['create_time'].values.astype('<M8[D]')
    min_date = df['create_time'].min()
    lenght_of_day = abs(datetime(2020, 3, 10) - min_date).days

    df1 = pd.DataFrame({'create_time':pd.date_range(min_date, periods=lenght_of_day),
                        'Active_professionals_count': 0})


    # iterate through each professional id
    for i in np.unique(df.proffesional_id):

        trans_dict = df[df.proffesional_id == i]

        # end date of the available snapshot
        enddate_final = datetime(2020, 3, 10)


        # this section deals with professional id having multiple event_type
        if len(trans_dict)>1:
            number_of_counts  = len(trans_dict)
            trans_dict = trans_dict.sort_values(by="create_time")

            for i,row in trans_dict.iterrows():
                if row.event_type_id == 1 and number_of_counts > 0:
                    startdate = row.create_time
                    number_of_counts -=1
                elif row.event_type_id == 2:
                    enddate = row.create_time
                    number_of_counts -=1
                    update_data(df1, startdate, enddate)
                if row.event_type_id == 1 and number_of_counts == 0:
                    startdate = row.create_time
                    enddate = enddate_final
                    update_data(df1, startdate, enddate)
        # this section deals with professional id having single event_type
        else:

            trans_dict = trans_dict.to_dict(orient='records')[0]
            startdate = trans_dict['create_time']
            enddate = enddate_final
            update_data(df1, startdate, enddate)

    return df1

def create_table(cur, conn):
    """
    Run's create table queries defined in this section
    :param cur: cursor to the database
    :param conn: database connection reference
    """

    # create a table availability_snapshot
    availability_snapshot_table_create = ("""CREATE TABLE IF NOT EXISTS availability_snapshot(
	create_time  TIMESTAMP,
	Active_professionals_count  INT
    )""")
    cur.execute(availability_snapshot_table_create)
    conn.commit()

def execute_many(conn, df, table):
    """
    Using cursor.executemany() to insert the dataframe
    """
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns

    cols = ','.join(list(df.columns))
    # SQL quert to execute
    query  = "INSERT INTO %s(%s) VALUES(%%s,%%s)" % (table, cols)
    cursor = conn.cursor()
    try:
        cursor.executemany(query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("execute_many() done")
    cursor.close()


def main():
    """
    Driver function for loading question 2 to create a availability snapshot tables
    """
    cur, conn = connect_db()

    create_table(cur, conn)

    print("Table created successfully!!")

    extracted_df= extract_data(conn)

    processed_dataframe = process_data(extracted_df)
    print(" processed successfully!!")
    
    execute_many(conn, processed_dataframe, "availability_snapshot")
    print("availability_snapshot Table created successfully!!")

    conn.close()

if __name__ == "__main__":
    main()
    print("\n\nFinished processing!!!\n\n")