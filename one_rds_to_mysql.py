from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import pymysql.cursors

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 20),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def copy_table_from_redshift_to_rds():
    try:
        # Redshift 연결 설정
        redshift_hook = PostgresHook(postgres_conn_id='redshift_conn_id')
        
        # Redshift에서 데이터를 읽어옴
        redshift_connection = redshift_hook.get_conn()
        redshift_cursor = redshift_connection.cursor()
        redshift_cursor.execute("SELECT * FROM analytics.flight_offers")

        # MySQL 연결 설정
        rds_connection = pymysql.connect(host='***',
                                        user='***',
                                        password='***',
                                        database='***',
                                        cursorclass=pymysql.cursors.DictCursor)
        
        # MySQL로 데이터를 복사
        with rds_connection.cursor() as rds_cursor:
            for row in redshift_cursor.fetchall():
                sql = """
                    INSERT INTO one_way_flight (
                        number_Of_Bookable_Seats,
                        price,
                        departure_city_id,
                        departure_terminal,
                        departure_datetime,
                        arrival_city_id,
                        arrival_terminal,
                        arrival_datetime,
                        duration,
                        cabin_class,
                        carrier_code,
                        created_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                rds_cursor.execute(sql, (
                    row[0],
                    row[2],
                    row[3],
                    row[4],
                    row[5],
                    row[6],
                    row[7],
                    row[8],
                    row[9],
                    row[10],
                    row[11],
                    row[12]
                    
                ))
        
        # 변경사항 커밋
        rds_connection.commit()
        
        # 연결 닫기
        redshift_cursor.close()
        redshift_connection.close()
        rds_connection.close()

        print("Data copied successfully from Redshift to RDS.")
    except Exception as e:
        print(f"Error copying data from Redshift to RDS: {str(e)}")
        raise

with DAG('one_rds_to_mysql', default_args=default_args, description='Copy table from Redshift to RDS', schedule_interval=timedelta(days=1), catchup=False) as dag:
    
    copy_data_task = PythonOperator(
        task_id='copy_data_to_rds',
        python_callable=copy_table_from_redshift_to_rds,
        dag=dag
    )
    
    copy_data_task
