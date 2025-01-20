# Импортируем необходимые библиотеки
import requests as req  # для выполнения HTTP-запросов
import pandas as pd  # для обработки данных
from datetime import datetime, timedelta  # для работы с датами
import json  # для парсинга json
from clickhouse_driver import Client  # для подключения к ClickHouse
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import xml.etree.ElementTree as ET  # для парсинга XML


URL = 'http://www.cbr.ru/scripts/XML_daily.asp'  # URL для получения курсов валют с сайта ЦБ
DATE = '01/01/2022'

# Настройка подключения к базе данных ClickHouse
CH_CLIENT = Client(
    host='158.160.116.58',  # IP-адрес сервера ClickHouse
    user='student',  # Имя пользователя для подключения
    password='sdf4wgw3r',  # Пароль для подключения
    database='sandbox'  # База данных, к которой подключаемся
)

# Функция для извлечения данных с API Центрального банка и сохранения их в локальный файл
def extract_data(url, date, s_file):
    """
    Эта функция выгружает данные по валютам, используя GET-запрос,
    и сохраняет результат в локальный файл `s_file`.
    """
    request = req.get(f"{url}?date_req={date}")  # Выполняем GET-запрос для получения данных за указанную дату

    # Сохраняем полученные данные (в формате XML) в локальный файл
    with open(s_file, "w", encoding="utf-8") as tmp_file:
        tmp_file.write(request.text)  # Записываем текст ответа в файл

        
def transform_data(s_file, csv_file, date):
    """
    Эта функция обрабатывает полученные данные в формате XML
    и преобразует их в CSV формат для дальнейшей работы.
    """
    rows = list()  # Список для хранения строк данных

    # Устанавливаем парсер для XML с кодировкой UTF-8
    parser = ET.XMLParser(encoding="utf-8")
    # Чтение и парсинг XML файла
    tree = ET.parse(s_file, parser=parser).getroot()

    # Перебор всех валют в XML и извлечение нужных данных
    for child in tree.findall("Valute"):
        num_code = child.find("NumCode").text  # Номер валюты
        char_code = child.find("CharCode").text  # Символьный код валюты
        nominal = child.find("Nominal").text  # Номинал валюты
        name = child.find("Name").text  # Название валюты
        value = child.find("Value").text  # Значение валюты

        # Добавляем полученные данные в список строк
        rows.append((num_code, char_code, nominal, name, value))

    # Создание DataFrame из полученных данных
    data_frame = pd.DataFrame(
        rows, columns=["num_code", "char_code", "nominal", "name", "value"]
    )

    # Добавляем колонку с датой, чтобы отслеживать, когда данные были получены
    data_frame['date'] = date

    # Сохраняем данные в CSV файл
    data_frame.to_csv(csv_file, sep=",", encoding="utf-8", index=False)
        
def upload_to_clickhouse(csv_file, table_name, client):

    # Чтение данных из CSV
    data_frame = pd.read_csv(csv_file)

    # Создание таблицы, ЕСЛИ НЕ СУЩЕСТВУЕТ ТО СОЗДАТЬ ТАБЛИЦУ
    client.execute(f'CREATE TABLE IF NOT EXISTS {table_name} (num_code Int64, char_code String, nominal Int64, name String, value String, date String) ENGINE Log')

    # Запись data frame в ClickHouse
    client.execute(f'INSERT INTO {table_name} VALUES', data_frame.to_dict('records'))
    
with DAG(
    'CBRF_NIKOS',
    schedule_interval=None,  # Запуск каждый день
    start_date=datetime(2024,1,1)  
) as dag:
    # Операторы для выполнения шагов
    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=extract_data,
        op_args=[URL, DATE, 'currency'],
        dag=dag
    )
    
    transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        op_args=['currency', 'currency.csv', DATE],
        dag=dag
    )
    upload_to_clickhouse = PythonOperator(
        task_id='upload_to_clickhouse',
        python_callable=upload_to_clickhouse,
        op_args=['currency.csv', 'CBR_NIKOS2', CH_CLIENT],
        dag=dag
    )

extract_task >> transform_data >> upload_to_clickhouse
 
