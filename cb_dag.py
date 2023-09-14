
import sqlalchemy as sa
from urllib.parse import quote
import datetime as dt
from dateutil.relativedelta import relativedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.hooks.base import BaseHook
from airflow.operators.dummy import DummyOperator
from airflow.contrib.operators.vertica_operator import VerticaOperator

from cb_etl.scripts.collable import etl


source_con = BaseHook.get_connection('cb')
api_endpoint = source_con.host

get_stavka = """<?xml version="1.0" encoding="utf-8"?>
            <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
            <soap:Body>
                <KeyRateXML xmlns="http://web.cbr.ru/">
                <fromDate>{0}</fromDate>
                <ToDate>{1}</ToDate>
                </KeyRateXML>
            </soap:Body>
            </soap:Envelope>"""

get_news = """<?xml version="1.0" encoding="utf-8"?>
            <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
            <soap:Body>
                <NewsInfoXML xmlns="http://web.cbr.ru/">
                <fromDate>{0}</fromDate>
                <ToDate>{1}</ToDate>
                </NewsInfoXML>
            </soap:Body>
            </soap:Envelope>"""

get_usa_course = """<?xml version="1.0" encoding="utf-8"?>
            <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
            <soap:Body>
                <GetCursDynamicXML xmlns="http://web.cbr.ru/">
                <FromDate>{0}</FromDate>
                <ToDate>{1}</ToDate>
                <ValutaCode>R01235</ValutaCode>
                </GetCursDynamicXML>
            </soap:Body>
            </soap:Envelope>"""

headers = {'content-type': 'text/xml'}

dwh_con = BaseHook.get_connection('vertica')
ps = quote(dwh_con.password)
dwh_engine = sa.create_engine(
    f'vertica+vertica_python://{dwh_con.login}:{ps}@{dwh_con.host}:{dwh_con.port}/sttgaz'
)

default_args = {
    'owner': 'Швейников Андрей',
    'email': ['xxxRichiexxx@yandex.ru'],
    'retries': 3,
    'retry_delay': dt.timedelta(minutes=30),
}
with DAG(
        'cb_data',
        default_args=default_args,
        description='Получение данных из ЦЕНТРОБАНКА.',
        start_date=dt.datetime(2022, 1, 1),
        schedule_interval='@monthly',
        catchup=True,
        max_active_runs=1
) as dag:

    start = DummyOperator(task_id='Начало')

    with TaskGroup('Загрузка_данных_в_stage_слой') as data_to_stage:

        get_stavka = PythonOperator(
                    task_id=f'get_stavka',
                    python_callable=etl,
                    op_kwargs={
                        'data_type': 'stage_cb_stavka',
                        'api_endpoint': api_endpoint,
                        'dwh_engine': dwh_engine,
                        'method': 'post',
                        'headers': headers,
                        'post_data': get_stavka,
                        'xpath': "//KR",
                    },
                )
        
        get_news = PythonOperator(
                    task_id=f'get_news',
                    python_callable=etl,
                    op_kwargs={
                        'data_type': 'stage_cb_news',
                        'api_endpoint': api_endpoint,
                        'dwh_engine': dwh_engine,
                        'method': 'post',
                        'headers': headers,
                        'post_data': get_news,
                        'xpath': "//News",
                    },
                )
    
        get_usa_course = PythonOperator(
                    task_id=f'get_news',
                    python_callable=etl,
                    op_kwargs={
                        'data_type': 'stage_cb_usa_course',
                        'api_endpoint': api_endpoint,
                        'dwh_engine': dwh_engine,
                        'method': 'post',
                        'headers': headers,
                        'post_data': get_usa_course,
                        'xpath': "//ValuteCursDynamic",
                    },
                )
        
        [get_stavka, get_news, get_usa_course]

    with TaskGroup('Загрузка_данных_в_dds_слой') as data_to_dds:

        pass

    with TaskGroup('Загрузка_данных_в_dm_слой') as data_to_dm:

        pass

    with TaskGroup('Проверки') as data_checks:

        pass

    end = DummyOperator(task_id='Конец')

    start >> data_to_stage >> data_to_dds >> data_to_dm >> data_checks >> end
