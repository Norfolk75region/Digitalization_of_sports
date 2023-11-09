import json
import requests
from urllib.parse import urljoin
import pandas as pd
import datetime
import re

from airflow.models import Variable
from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# from airflow.hooks import PostgresHook
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator


# import airflow

def get_data():
    """
    Получение данных из файла

    :return:
    """
    pass


def transform_data(file_name='Ульяновская область, отчет за 21.08.23.xlsx', date=datetime.date(2023, 8, 21)):
    inn = {
        "МАУ ДО ДЮСШ Новомалыклинского района": "7312003874",
        "ОГБУ ДО \"СШОР по художественной гимнастике\"": "7325034750",
        "ОГБУ ДО \"УСШОР по биатлону\"": "7326054886",
        "МБУ ДО СШ им.Д.А.Разумовского": "7328077744",
        "МБУ ДО ДЮСШ МО \"Мелекесский р-н Ульяновской обл.\"": "7329011062",
        "ОГБУ ДО \"СШОР по лёгкой атлетике им. А.С. Ларина\"": "7303020312",
        "ОГКУ ДО \"СШОР по спорт. борьбе им. А.И. Винника\"": "7325088924",
        "ОГБУ ДО \"СШОР по хоккею с мячом\"": "7327040357",
        "МБУ «СШ «Борец»": "7326021111",
        "МБУ ДО «СШ «Борец»": "7326021111",
        "МБУ ДО «СШ «Симбирск»": "7325045656",
        "МБУ «СШ №4»": "7303020672",
        "МБУ ДО «СШ №4»": "7303020672",
        "МБУДО «СШ Засвияжского района»": "7326021129",
        "ОГБУ «СШОР по боксу имени П.Т. Липатова»": "7326053603",
        "МБУ «СШОР «Рингстар»": "7328049578",
        "МБУ СШ \"ЛАДА\" г. Димитровград": "7302042874",
        "МБУ «СШОР «Юность»": "7328509994",
        "МБУ ДО «СШОР «Юность»": "7328509994",
        "МАУ ДО СШ \"Цементник\"": "7321315125",
        "МБУ ДО\"СШОР \"Волга\"": "7328049592",
        "МБУ ДО «СШ «Старт»": "7328061085",
        "МБУ «СШ «Фаворит»": "7328084283",
        "МБУ «СШ №9»": "7328041635",
        "МБУ ДО «СШ №9»": "7328041635",
        "МБУ «СШОР № 6»": "7303022535",
        "ОГБУ ДО \"СШОР по боксу им.А.В.Гришина\"": "7302032587",
        "ОГБУ ДО\" СШОР по тхэквондо\"": "7326011699",
        "МКУ ДО СШ \"Старт\"": "7329005439",
        "ОГБУ ДО \"СШ по хоккею \"Лидер\"": "7328084357",
        "МУ \"СШ по хоккею и тхэквондо \"Олимп\"": "7313007060",
        "ОГБУ «СШ по футболу «Волга» им. Н.П. Старостина»": "7325099370",
        "ОГБУДО «СШ по футболу «Волга» им. Н.П. Старостина»": "7325099370",
        "МБУ ДО МЧСШ": "7329031037",
        "ОГБ ФСУ ДО \"СШННВС\"": "7327062216",
        "МБУ ДО СШ им. Ж.Б.Лобановой": "7302030413",
        "ОГБПОУ \"УУ(т)ОР\"": "7303012865",
        "МБУ ДО ФОСЦ «ОРИОН»": "7328512267",
        "ОГКУДО \"УСАШ ПСР\"": "7327080350",
        "МБУ «СШ №1»": "7325025843",
        "ОГКУ ДО СШ по дзюдо \"Спарта\"": "7327030158",
    } #словарь для маппинга имени организации в ИНН
    column_name = {
        1: 'INN',
                   2: 'fact_employee',
                   3: 'fact_act_employee',
                   4: 'fact_trainers',
                   5: 'fact_act_trainers',
                   6: 'trainers_training',
                   7: 'act_trainers_training',
                   11: 'sportsmans',
                   12: 'act_sportsmans',
                   13: 'sportmans_old',
                   14: 'sportmans_act_training',
                   15: 'sportmans_no_group',
                   16: 'noactive_sportsmans_act_training',
                   19: 'parents',
                   20: 'act_parents',
                   21: 'expect_parents',
                   22: 'parents_child_training',
                   23: 'parents_child_training_act',
                   28: 'all_groups',
                   29: 'groups_no_sportsmans',
                   30: 'groups_no_active_sportsmans',
                   31: 'groups_no_act_trainer',
                   33: 'schedule_sportsmans_PER',
                   36: 'group_schedule_PER',
                   38: 'journaling_PER',
                   39: 'attendance_PER',
                   40: 'absence_marks',
                   41: 'training_plan'
                   }
    # Получаем дату отчёта из имени файла
    date = re.findall('\d\d\.\d\d\.\d\d', file_name)
    date = datetime.datetime.strptime(date[0], '%d.%m.%y')
    data_frame = pd.read_excel(file_name)
    data_frame = data_frame.iloc[3:39, 2:]  # Забираем строки и столбцы с данными
    data_frame.columns = range(data_frame.columns.size)  # Переименовываем название столбцов 0-41
    data_frame.reset_index(drop=True, inplace=True)  # сбрасываем нумерацию строк
    data_frame = data_frame.rename(columns=column_name)  # присваиваем колонкам имена из словаря
    data_frame['INN'] = data_frame['INN'].replace(inn)  # заменяем названия организаций на ИНН
    data_frame = pd.DataFrame(
        (data_frame[series] for series in column_name.values())).T  # заменяем датафрейм согласно словарю с таблицами
    data_frame = data_frame.applymap(lambda cell_value: re.sub(r'\D', '', str(cell_value)))#Заменяем все значения в ячейках через lambda функцию
    data_frame['date'] = date.strftime('%Y-%m-%d')
    data_frame.to_json('inn.json')  # не обязателен
    data_frame.to_csv(f'sport_today.csv', sep=';')
    return


def insert_data(**kwargs):
    """
    Вставляем данные в базу

    :param kwargs:
    :return:
    """
    data = pd.read_json('inn.json')
    # pg_hook = PostgresHook(postgress_conn_id='ul_db')
    for index, row in data.iterrows():
        columns = f'{", ".join(row.keys())}'
        values = f'{", ".join(str(val) for val in row)}'[:-9]
        sql_request = f'INSERT INTO "DC"."sports_organization_info" ({columns}) ' \
                      f'VALUES ({values})'
        print(sql_request)
        # pg_hook.run(insert_data())


with DAG(
        'Sport_cifrovizaciya', #название дага
        schedule_interval='10 9 * * *', #периодичность запуска согласно cron
        start_date=days_ago(1), #атрибут необходимости предыдущих дат
        catchup=False
) as dag:
    get_data_task = PythonOperator(
        task_id='get_data', #имя таска
        python_callable=get_data, #название функции
        provide_context=True, #не помню зачем
        do_xcom_push=True #может ли прокидывать данные
    )
    convert_data_task = PythonOperator(
        task_id='convert_data',
        python_callable=transform_data,
        provide_context=True,
        do_xcom_push=True
    )
    insert_task = PythonOperator(
        task_id='insert_data_in_SQL',
        python_callable=insert_data,
        provide_context=True,
        do_xcom_push=True
    )
    get_data_task >> convert_data_task >> insert_task #порядок выполнения тасков

if __name__ == '__main__':
    get_data()
    transform_data()
    insert_data()
