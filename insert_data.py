import pandas as pd
from airflow.hooks import PostgresHook
import os, glob
from r5.helpers.general import Logger

log = Logger()

def main(**kwargs):
    '''
    Вставляем данные в базу

    :param kwargs:
    :return:
    '''
    data = pd.read_json('inn.json')
    pg_hook = PostgresHook(postgress_conn_id='ul_db')
    for index, row in data.iterrows():
        columns = f'{", ".join(row.keys())}'
        values = f'{", ".join(str(val) for val in row)}'[:-9]
        sql_request = f'INSERT INTO "DC"."sports_organization_info" ({columns}) ' \
                      f'VALUES ({values})'
        log.info(sql_request)
        pg_hook.run(sql_request)

full_dict_path = '/app/cache'+os.getenv('AF_LOAD_PATH')

for file_name in glob.glob(os.path.join(full_dict_path, '*.csv')):
    main(file_name)
