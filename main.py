from create_tables import main as create_table_main
from etl import main as etl_main
from availability_snapshot import main as availability_snapshot_main

if __name__ == "__main__":
    create_table_main()
    etl_main()
    print("ETL Pipeline cerated !!")
    "------------------------------------------------------------------------------------"
    print("Now answering Question 2 in order to create a availability snapshot table !!")
    availability_snapshot_main()