from airflow.models import DAG
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
import pandas as pd
import logging
from datetime import datetime
import csv
import os

CSV_PATH_RAW_DATA = '/opt/airflow/dags/taxi.csv'
CSV_PATH_PROCESSED_RAW_DATA = '/opt/airflow/dags/raw_trip_data.csv'
CSV_PATH_MONTHLY_TRIPS = '/opt/airflow/dags/monthly_trips.csv'
CSV_PATH_DAILY_TRIPS = '/opt/airflow/dags/daily_trips.csv'

default_args = {
    'start_date': datetime(2024, 1, 20)
}

with DAG('Final_DATA_Assignment_Rafi', schedule_interval='0 6 * * *',
         default_args=default_args,
         catchup=False) as dag:

    def raw_data_processing():
        try:
            df_raw = pd.read_csv(CSV_PATH_RAW_DATA)
            df_raw.drop(columns=['Unnamed: 0'], inplace=True)
            df_raw['tpep_pickup_datetime'] = pd.to_datetime(df_raw['tpep_pickup_datetime'])
            df_raw['tpep_dropoff_datetime'] = pd.to_datetime(df_raw['tpep_dropoff_datetime'])
            df = df_raw[(df_raw['tpep_pickup_datetime'].dt.year == 2023) | (df_raw['tpep_dropoff_datetime'].dt.year == 2023)]
            df['trip_period'] = df['tpep_pickup_datetime'].dt.strftime('%Y-%m')
            df.to_csv(CSV_PATH_PROCESSED_RAW_DATA, index=False)
        except Exception as e:
            logging.error(f"Error in raw_data_processing: {e}")

    raw_trip_data_processing = PythonOperator(
        task_id='raw_data_processing',
        python_callable=raw_data_processing,
        dag=dag,
    )

    def monthly_data_processing():
        try:
            raw_df = pd.read_csv(CSV_PATH_PROCESSED_RAW_DATA)
            monthly_trips = raw_df.groupby('trip_period').agg({
                'tpep_pickup_datetime': 'count',            
                'tpep_dropoff_datetime': lambda x: (pd.to_datetime(x).max() - pd.to_datetime(x).min()).total_seconds(),  
                'passenger_count': ['sum', 'mean'],         
                'total_amount': ['sum', 'mean'],            
                'tip_amount': ['count', 'sum', 'mean'],     
                'congestion_surcharge': 'sum',              
                'payment_type': lambda x: x.mode()[0],      
                'Airport_fee': ['count', 'sum']             
            }).reset_index()

            monthly_trips.columns = ['date', 'num_trips', 'total_duration', 'total_passenger', 'avg_passenger',
                                    'total_fares', 'avg_fares', 'count_tips', 'total_tips', 'avg_tips',
                                    'total_surcharges', 'most_payment_type', 'count_to_airport', 'total_airport_fee']
            monthly_trips['date'] = pd.to_datetime(monthly_trips['date']).dt.strftime('%Y-%m-%d')

            monthly_trips['avg_duration'] = monthly_trips['total_duration'] / monthly_trips['num_trips']
            desired_order = ['date', 'num_trips', 'total_duration', 'avg_duration', 'total_passenger', 'avg_passenger',
                            'total_fares', 'avg_fares', 'count_tips', 'total_tips', 'avg_tips',
                            'total_surcharges', 'most_payment_type', 'count_to_airport', 'total_airport_fee']
            monthly_trips = monthly_trips[desired_order]
            monthly_trips.to_csv(CSV_PATH_MONTHLY_TRIPS, index=False)
        except Exception as e:
            logging.error(f"Error in monthly_data_processing: {e}")

    task_monthly_data_processing = PythonOperator(
        task_id='monthly_data_processing',
        python_callable=monthly_data_processing,
        dag=dag,
    )

    def daily_data_processing():
        try:
            daily_df = pd.read_csv(CSV_PATH_PROCESSED_RAW_DATA)
            daily_df['trip_period_daily'] = pd.to_datetime(daily_df['tpep_pickup_datetime']).dt.strftime('%Y-%m-%d')
            daily_trips = daily_df.groupby('trip_period_daily').agg({
                'tpep_pickup_datetime': 'count',           
                'tpep_dropoff_datetime': lambda x: (pd.to_datetime(x).max() - pd.to_datetime(x).min()).total_seconds(),  
                'passenger_count': ['sum', 'mean'],         
                'total_amount': ['sum', 'mean'],            
                'tip_amount': ['count', 'sum', 'mean'],     
                'congestion_surcharge': 'sum',              
                'payment_type': lambda x: x.mode()[0],      
                'Airport_fee': ['count', 'sum']             
            }).reset_index()
            daily_trips.columns = ['date', 'num_trips', 'total_duration', 'total_passenger', 'avg_passenger',
                                'total_fares', 'avg_fares', 'count_tips', 'total_tips', 'avg_tips',
                                'total_surcharges', 'most_payment_type', 'count_to_airport', 'total_airport_fee']
            daily_trips['avg_duration'] = daily_trips['total_duration'] / daily_trips['num_trips']
            desired_order = ['date', 'num_trips', 'total_duration', 'avg_duration', 'total_passenger', 'avg_passenger',
                            'total_fares', 'avg_fares', 'count_tips', 'total_tips', 'avg_tips',
                            'total_surcharges', 'most_payment_type', 'count_to_airport', 'total_airport_fee']
            daily_trips = daily_trips[desired_order]
            daily_trips.to_csv(CSV_PATH_DAILY_TRIPS, index=False)
        except Exception as e:
            logging.error(f"Error in daily_data_processing: {e}")

    task_daily_data_processing = PythonOperator(
        task_id='daily_data_processing',
        python_callable=daily_data_processing,
        dag=dag,
    )

    drop_table_raw = SqliteOperator(
        task_id='drop_table_raw',
        sqlite_conn_id='sqlite_default',  
        sql="DROP TABLE IF EXISTS raw_trip_data;"
    )

    drop_table_daily = SqliteOperator(
        task_id='drop_table_daily',
        sqlite_conn_id='sqlite_default',  
        sql="DROP TABLE IF EXISTS daily_trips;"
    )

    drop_table_monthly = SqliteOperator(
        task_id='drop_table_monthly',
        sqlite_conn_id='sqlite_default',  
        sql="DROP TABLE IF EXISTS monthly_trips;"
    )

    creating_table_raw_trip_data = SqliteOperator(
        task_id='creating_table_raw_trip_data',
        sqlite_conn_id='sqlite_default',
        sql='''
            CREATE TABLE IF NOT EXISTS raw_trip_data (
            VendorID INTEGER, 
            tpep_pickup_datetime DATETIME, 
            tpep_dropoff_datetime DATETIME, 
            passenger_count FLOAT, 
            trip_distance FLOAT, 
            RatecodeID FLOAT, 
            store_and_fwd_flag STRING, 
            PULocationID INTEGER, 
            DOLocationID INTEGER, 
            payment_type INTEGER, 
            fare_amount FLOAT, 
            extra FLOAT, 
            mta_tax FLOAT, 
            tip_amount FLOAT, 
            tolls_amount FLOAT, 
            improvement_surcharge FLOAT, 
            total_amount FLOAT, 
            congestion_surcharge FLOAT, 
            Airport_fee FLOAT,
            trip_period STRING
            );
        '''
    )

    creating_table_monthly_trips = SqliteOperator(
        task_id='creating_table_monthly_trips',
        sqlite_conn_id='sqlite_default',
        sql='''
            CREATE TABLE IF NOT EXISTS monthly_trips (
            date DATETIME, 
            num_trips INT,
            total_duration FLOAT,
            avg_duration FLOAT,
            total_passenger INT,
            avg_passenger INT,
            total_fares FLOAT,
            avg_fares FLOAT,
            count_tips INTEGER,
            total_tips FLOAT,
            avg_tips FLOAT,
            total_surcharges FLOAT,
            most_payment_type INTEGER,
            count_to_airport INTEGER,
            total_airport_fee FLOAT
            );
        '''
    )

    creating_table_daily_trips = SqliteOperator(
        task_id='creating_table_daily_trips',
        sqlite_conn_id='sqlite_default',
        sql='''
            CREATE TABLE IF NOT EXISTS daily_trips (
            date DATETIME, 
            num_trips INT,
            total_duration FLOAT,
            avg_duration FLOAT,
            total_passenger INT,
            avg_passenger INT,
            total_fares FLOAT,
            avg_fares FLOAT,
            count_tips INTEGER,
            total_tips FLOAT,
            avg_tips FLOAT,
            total_surcharges FLOAT,
            most_payment_type INTEGER,
            count_to_airport INTEGER,
            total_airport_fee FLOAT
            );
        '''
    )

    query_insert_raw_data = '''
        INSERT INTO raw_trip_data (VendorID, tpep_pickup_datetime, tpep_dropoff_datetime,
        passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID,
        payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge,
        total_amount, congestion_surcharge, Airport_fee, trip_period) VALUES (?,?,?,?,?,?,?,?,?,?,
        ?,?,?,?,?,?,?,?,?,?)
    '''

    query_insert_monthly_trips = '''
        INSERT INTO monthly_trips (date, num_trips, total_duration, avg_duration,
        total_passenger, avg_passenger, total_fares, avg_fares, count_tips, total_tips,
        avg_tips, total_surcharges, most_payment_type, count_to_airport, total_airport_fee) 
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    '''

    query_insert_daily_trips = '''
        INSERT INTO daily_trips (date, num_trips, total_duration, avg_duration,
        total_passenger, avg_passenger, total_fares, avg_fares, count_tips, total_tips,
        avg_tips, total_surcharges, most_payment_type, count_to_airport, total_airport_fee) 
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    '''

    def import_csv_data():
        hook = SqliteHook(sqlite_conn_id='sqlite_default')
        with hook.get_conn() as conn:
            cursor = conn.cursor()
            _insert_data_from_csv(cursor, CSV_PATH_PROCESSED_RAW_DATA, query_insert_raw_data)
            _insert_data_from_csv(cursor, CSV_PATH_MONTHLY_TRIPS, query_insert_monthly_trips)
            _insert_data_from_csv(cursor, CSV_PATH_DAILY_TRIPS, query_insert_daily_trips)
            conn.commit()

    def _insert_data_from_csv(cursor, csv_path, query):
        try:
            with open(os.path.abspath(csv_path), 'r') as csvfile:
                reader = csv.reader(csvfile)
                next(reader)  
                for row in reader:
                    cursor.execute(query, row)
        except Exception as e:
            logging.error(f"Error while inserting data from {csv_path}: {e}")

    import_csv_task = PythonOperator(
        task_id='import_csv_data',
        python_callable=import_csv_data,
        dag=dag,
    )

    drop_table_raw >> creating_table_raw_trip_data
    drop_table_monthly >> creating_table_monthly_trips
    drop_table_daily >> creating_table_daily_trips

    raw_trip_data_processing >> task_monthly_data_processing
    task_monthly_data_processing >> task_daily_data_processing
    task_daily_data_processing >> import_csv_task

