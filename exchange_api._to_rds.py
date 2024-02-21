from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime, timedelta

import requests
import logging
import json


def get_rds_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='aws_rds')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()


@task
def etl(schema, table, api_key):
    # 환율API 사용
    url = f"https://www.koreaexim.go.kr/site/program/financial/exchangeJSON?authkey={api_key}&data=AP01"
    response = requests.get(url)
    data = response.json()
    
    if data is None:
        logging.warning("API로부터 데이터를 가져오지 못했습니다. DAG 실행을 중단합니다.")
        return
    
    cur = get_rds_connection()
    
    check_table_sql = f"SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = '{schema}' AND table_name = '{table}')"
    cur.execute(check_table_sql)
    table_exists = cur.fetchone()[0]
    if not table_exists:
    # 새로운 테이블 생성
        create_table_sql = f"""CREATE TABLE {schema}.{table} (
            update TIMESTAMP,
            cur_unit VARCHAR(10),
            ttb FLOAT,
            tts FLOAT,
            deal_bas_r FLOAT,
            bkpr INTEGER,
            yy_efee_r INTEGER,
            ten_dd_efee_r INTEGER,
            kftc_bkpr INTEGER,
            kftc_deal_bas_r FLOAT,
            cur_nm VARCHAR(100)
        );"""
        logging.info(create_table_sql)
        cur.execute(create_table_sql)
    now = datetime.now()
    update = now.strftime("%Y-%m-%d %H:%M")
    # 데이터 입력
    for item in data:
        cur_unit = item['cur_unit']
        ttb = float(item['ttb'].replace(',', ''))
        tts = float(item['tts'].replace(',', ''))
        deal_bas_r = float(item['deal_bas_r'].replace(',', ''))
        bkpr = int(item['bkpr'].replace(',', ''))
        yy_efee_r = int(item['yy_efee_r'].replace(',', ''))
        ten_dd_efee_r = int(item['ten_dd_efee_r'].replace(',', ''))
        kftc_bkpr = int(item['kftc_bkpr'].replace(',', ''))
        kftc_deal_bas_r = float(item['kftc_deal_bas_r'].replace(',', ''))
        cur_nm = item['cur_nm']
        insert_sql = f"INSERT INTO {schema}.{table} (update, cur_unit, ttb, tts, deal_bas_r, bkpr, yy_efee_r, ten_dd_efee_r, kftc_bkpr, kftc_deal_bas_r, cur_nm) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
        insert_values = (update, cur_unit, ttb, tts, deal_bas_r, bkpr, yy_efee_r, ten_dd_efee_r, kftc_bkpr, kftc_deal_bas_r, cur_nm)
        logging.info(insert_sql)
        try:
            cur.execute(insert_sql, insert_values)
            cur.execute("COMMIT;")
        except Exception as e:
            cur.execute("ROLLBACK;")
            raise



with DAG(
    dag_id = 'exchange_rate_info',
    start_date = datetime(2024,2,12), # 날짜가 미래인 경우 실행이 안됨
    schedule = '0 * * * *',  
    max_active_runs = 1,
    catchup = False,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
        'owner': 'jaewoo'
    }
) as dag:

    etl('public','exchange_rate',Variable.get("exchange_rate_api_key"))