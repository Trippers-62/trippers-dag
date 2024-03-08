import requests
import json
import logging, time
import codecs
import pendulum
import sys
import os  # 시스템

import pandas as pd  # 판다스 : 데이터분석 라이브러리
import numpy as np   # 넘파이 : 숫자, 행렬 데이터 라이브러리

from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from amadeus import ResponseError, Client

from bs4 import BeautifulSoup    # html 데이터를 전처리


default_args = {
    'owner': 'ohyoung',
    'start_date': pendulum.yesterday(),
    'provide_context': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=3)
}

offsets = [0, 25, 50, 75]

@task
def hotel_list_scraping_okinawa():
    hotel_results = []
    # Step 1. 크롬 웹브라우저 실행
    #chrome_options = Options()
    #chrome_options.add_argument("--headless")
    #chrome_options.add_argument("--disable-gpu")
    headers={"User-Agent":"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.5042.108 Safari/537.36"}
 

    checktime = datetime.now() + timedelta(days=30)

    for i in range(1, 11):#(1, 11)
        for delta in range(i+1, 11):#(1, 11)
            for offset in offsets:
                checkin = (checktime + timedelta(days=i)).strftime("%Y-%m-%d")
                checkout = (checktime + timedelta(days=(delta))).strftime("%Y-%m-%d")
                url = "https://www.booking.com/searchresults.ko.html?ss=%EC%98%A4%ED%82%A4%EB%82%98%EC%99%80&ssne=%EC%82%BF%ED%8F%AC%EB%A1%9C&ssne_untouched=%EC%82%BF%ED%8F%AC%EB%A1%9C&label=ko-kr-booking-desktop-fDL93Zuo5ymosKSjAtXB9gS652828999093%3Apl%3Ata%3Ap1%3Ap2%3Aac%3Aap%3Aneg%3Afi%3Atikwd-324456682700%3Alp1009871%3Ali%3Adec%3Adm&aid=2311236&lang=ko&sb=1&src_elem=sb&src=searchresults&dest_id=2351&dest_type=region&ac_position=0&ac_click_type=b&ac_langcode=ko&ac_suggestion_list_length=5&search_selected=true&search_pageview_id=3ab68b8332d101ae&ac_meta=GhAzYWI2OGI4MzMyZDEwMWFlIAAoATICa286DOyYpO2CpOuCmOyZgEAASgBQAA%3D%3D&checkin={}&checkout={}&group_adults=2&no_rooms=1&group_children=0&order=review_score_and_price&offset={}".format(checkin, checkout, offset)
                #url만 바꾸면 됨
                response = requests.get(url, headers=headers)
                soup = BeautifulSoup(response.content, 'html.parser')
                #WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.CSS_SELECTOR,'div.abcc616ec7.cc1b961f14.c180176d40.f11eccb5e8.ff74db973c'))).click()
        
                for el in soup.find_all("div", {"data-testid": "property-card"}):
                    if el.find(class_="a3b8729ab1") is not None:
                        rating = el.find(class_="a3b8729ab1").text.strip()
                    else:
                        rating = 0
                    if el.find(class_="abf093bdfe f45d8e4c32 d935416c47") is not None:
                        review_count = el.find(class_="abf093bdfe f45d8e4c32 d935416c47").text.strip()
                    else:
                        review_count = "1개 이용 후기"
                    hotel_results.append({
                        "name": el.find("div", {"data-testid": "title"}).text.strip(),
                        "link": el.find("a", {"data-testid": "title-link"})["href"],
                        "location": el.find("span", {"data-testid": "address"}).text.strip(),
                        "pricing": el.find("span", {"data-testid": "price-and-discounted-price"}).text.strip(),
                        "rating": rating,
                        "review_count": review_count,
                        "thumbnail": el.find("img", {"data-testid": "image"})['src'],
                        "room_unit" : el.find("div", {"data-testid": "recommended-units"}).find("h4").text.strip(),
                        "recommended_units" : el.find("div", {"data-testid": "recommended-units"}).find("ul").text.strip(),
                        "checkin" : checkin,
                        "checkout" : checkout,
                    })
                time.sleep(1)
    #print(len(hotel_results))

    now = datetime.now().strftime("%Y%m%d%H%M")
    file_name = f'raw_data_hotel/okinawa/okinawa_{now}.json'
    data_str = json.dumps(hotel_results, indent=4, ensure_ascii=False)
    s3_hook = S3Hook(aws_conn_id='aws_s3')
    s3_hook.load_string(
        string_data=data_str,
        key=file_name,
        bucket_name='de-6-2-bucket'
    )


with DAG(
    dag_id='hotel_rawdata_dag_okinawa',
    default_args=default_args,
    # Schedule to run specific timeline
    schedule_interval="30 14 * * *"
    #schedule_interval="@once"
) as dag:
    # 직접 작성
    load_raw_data_hotel_okinawa = hotel_list_scraping_okinawa()