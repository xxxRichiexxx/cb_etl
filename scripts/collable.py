# import requests
# import pandas as pd




# data = """<?xml version="1.0" encoding="utf-8"?>
#             <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
#             <soap:Body>
#                 <KeyRateXML xmlns="http://web.cbr.ru/">
#                 <fromDate>2020-01-01</fromDate>
#                 <ToDate>2023-09-01</ToDate>
#                 </KeyRateXML>
#             </soap:Body>
#             </soap:Envelope>"""
# headers = {'content-type': 'text/xml'}

# response = requests.post(
#     'https://cbr.ru/DailyInfoWebServ/DailyInfo.asmx',
#     data = data,
#     headers = headers,
#     verify=False,
# )
# response.raise_for_status()

# print(response.text)

# data = pd.read_xml(response.text, xpath="//KR")

# print(data)


import pandas as pd
import numpy as np
import datetime as dt
import requests
from pprint import pprint
import os
from sqlalchemy import text


def extract(
        source_engine=None,
        data_type=None,
        api_endpoint=None,
        method='get',
        params=None,
        headers=None,
        post_data=None,
        auth=None,
        json_key=None,
        xpath=None,
        start_date=None,
        end_date=None,
):
    """Извлечение данных из источника."""

    print('ИЗВЛЕЧЕНИЕ ДАННЫХ')

    # Если необходимо извлечь дынные из API:
    if api_endpoint:

        print('Извлекаем данные из апи')
        print('api_endpoint', api_endpoint, 'auth', auth, 'params', params, 'headers', headers)

        response = getattr(requests, method, 'get')(
            api_endpoint,
            auth=auth,
            params=params,
            headers=headers,
            data=post_data.format(start_date, end_date),
            verify=False,
        )

        response.raise_for_status()

        response.encoding = 'utf-8-sig'

        if headers.get('content-type') == 'text/xml':
            print('XPATH', xpath)
            data = pd.read_xml(response.text, xpath=xpath)
        else:
            data = pd.json_normalize(response.json()[json_key])

    # Если необходимо извлечь дынные из БД:
    elif source_engine:

        print('Извлекаем данные из БД')

        path = os.path.abspath(fr'{data_type}.sql')

        with open(path, 'r') as f:
            command = f.read().format(start_date, end_date)

        print(command)

        data = pd.read_sql_query(
            command,
            source_engine,
            dtype_backend='pyarrow',
        )

    pprint(data)
    return data


def transform(data, column_names=None, execution_date=None):
    """Преобразование/трансформация данных."""

    print('ТРАНСФОРМАЦИЯ ДАННЫХ')
    print('Исходные поля:',  data.columns)

    if not data.empty:
        if column_names:
            data.columns = column_names

        if execution_date:
            data['load_date'] = execution_date.replace(day=1)
    else:
        print('Нет новых данных для загрузки.')
    return data


def load(data, dwh_engine, data_type, start_date):
    """Загрузка данных в хранилище."""

    print('ЗАГРУЗКА ДАННЫХ')
    if not data.empty:

        print(data)

        command = f"""
            SELECT DROP_PARTITIONS(
                'sttgaz.{data_type}',
                '{start_date}',
                '{start_date}'
            );
        """
        print(command)

        dwh_engine.execute(command)

        data.to_sql(
            f'{data_type}',
            dwh_engine,
            schema='sttgaz',
            if_exists='append',
            index=False,
        )
    else:
        print('Нет новых данных для загрузки.')


def etl(
    source_engine=None,
    data_type=None,
    api_endpoint=None,
    method='get',
    params=None,
    headers=None,
    post_data=None,
    auth=None,
    json_key=None,
    xpath=None,
    dwh_engine=None,
    offset=None,
    column_names=None,
    column_to_check=None,
    **context
):
    """Запускаем ETL-процесс для заданного типа данных."""

    if offset:
        month = context['execution_date'].month - offset
        if month <= 0:
            month = 12 + month
            execution_date = context['execution_date'].date() \
                .replace(month=month, year=context['execution_date'].year-1, day=1)
        else:
            execution_date = context['execution_date'].date() \
                .replace(month=month, day=1)
    else:
        execution_date = context['execution_date'].date()

    start_date = execution_date.replace(day=1)
    end_date = (execution_date.replace(day=28) + dt.timedelta(days=4)) \
        .replace(day=1) - dt.timedelta(days=1)
    
    data = extract(
        source_engine,
        data_type,
        api_endpoint,
        method,
        params,
        headers,
        post_data,
        auth,
        json_key,
        xpath,
        start_date,
        end_date,
    )
    data = transform(data, column_names, start_date)

    load(data, dwh_engine, data_type, start_date)

    # if column_to_check:

    #     try:
    #         data[column_to_check] = data[column_to_check].str.strip()
    #         data = data.replace(r'^\s*$', np.nan, regex=True)
    #         data[column_to_check] = data[column_to_check].fillna(0).astype(np.int64)
    #         value = sum(data[column_to_check])
    #     except KeyError:
    #         value = 0

    #     context['ti'].xcom_push(
    #         key=data_type,
    #         value=value
    #     )







# data = """<?xml version="1.0" encoding="utf-8"?>
#             <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
#             <soap:Body>
#                 <KeyRateXML xmlns="http://web.cbr.ru/">
#                 <fromDate>2020-01-01</fromDate>
#                 <ToDate>2023-09-01</ToDate>
#                 </KeyRateXML>
#             </soap:Body>
#             </soap:Envelope>"""
# headers = {'content-type': 'text/xml'}

# response = requests.post(
#     'https://cbr.ru/DailyInfoWebServ/DailyInfo.asmx',
#     data = data,
#     headers = headers,
#     verify=False,
# )
# response.raise_for_status()

# print(response.text)

# data = pd.read_xml(response.text, xpath="//KR")

# print(data)


# extract(
#     api_endpoint='https://cbr.ru/DailyInfoWebServ/DailyInfo.asmx',
#     method='post',
#     headers=headers,
#     post_data=data,
#     xpath="//KR",
#     start_date=None,
#     end_date=None,    
# )
