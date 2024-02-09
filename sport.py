import datetime
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow.operators.docker_operator import DockerOperator
from r5.helpers import serialize_connection  # @UnresolvedImport

default_args = {
    'owner': 'Airflow',
    'description': 'Sport Cifrovizaciya',
    'depend_on_past': False,
    'start_date': datetime.datetime(2019, 6, 2),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2
}

# пути к томам должны быть объявлены как Airflow Variables
host_volumes = Variable.get('host_environment', deserialize_json=True)['volumes']
volume_share = f'{host_volumes["share"]}:/app/share'
volume_cache = f'{host_volumes["cache"]}:/app/cache'
volume_source = f'{host_volumes["source"]}:/app/source'
volume_metadata = f'{host_volumes["metadata"]}:/app/metadata'

volumes = [volume_cache, volume_source, volume_metadata]

# общий префикс переменных окружения для упрощения переключения БД
db_prefix = 'DWH_DB_'
dag_id = 'sport_cifrovizacia'

db_conn = BaseHook.get_connection("ul_db")

environment = {
    'AF_EXECUTION_DATE': '{{ ds }}',
    'AF_TASK_ID': '{{ task.task_id }}',
    'AF_TASK_OWNER': '{{ task.owner }}',
    'AF_RUN_ID': '{{ run_id }}',
    'AF_DWH_DB_DIALECT': 'postgresql',
    'AF_DWH_DB_NAME': 'ul_db',
    'AF_DWH_DB_USER': db_conn.login,
    'AF_DWH_DB_PASSWORD': db_conn.password,
    'AF_DWH_DB_PORT': db_conn.port,
    'AF_DWH_DB_SERVER': db_conn.host,
    'AF_DWH_DB_SCHEMA': 'raw_data',
    'AF_LOGLEVEL': 'debug'
}

bucket_name = 'MinSport'
bucket_tokens = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJidWNrZXQiOiIzNzBkYjgxZi03Y2FiLTQ0OGEtYTJlMi1iMDFiNGQ5YzVhNjMiLCJjYXBhYmlsaXRpZXMiOlsibGlzdEZpbGVzIiwicmVhZEZpbGVzIiwid3JpdGVGaWxlcyIsImRlbGV0ZUZpbGVzIl19.R-l58LUXazqnRysPsxBVTMZZr4eqx1qYPe3-jxaPcgM'

python_path = '/app/source/python/ul_sport'

# настройки для загрузки из DWASH

load_directory = f'/sport/to_load'

def dwash_loader(dag, name, token, bucket_name, loader_storage):
    t = DockerOperator(
        dag=dag,
        task_id=f'{dag_id}.load_from_DWASH',
        image='r5_operator:latest',
        api_version='auto',
        auto_remove=True,
        environment={**environment,
                     **{'AF_PROVIDER_CONNECTION': serialize_connection('dwash_ulyanovsk'),
                        'AF_LOADER_TOKEN': token,
                        # все последующие переменные переопределяют соотвествующие Connection.Extra (без 'AF_LOADER_')
                        'AF_LOADER_RUN': 'ONCE',  # 'ONCE' - однократно, 'WAIT' - до результатов, 'REPEAT' - повторять
                        # 'AF_LOADER_CONTAINER': bucket_name,
                        'AF_LOADER_INTERVAL': 30,  # интервал в секундах между проверками наличия файлов по маске
                        'AF_LOADER_TIMEOUT': 300,  # максимальное время ожидания появления файла (в секундах)
                        'AF_LOADER_PROVIDER': 'DWASH',  # код провайдера (известного лоадера)
                        'AF_LOADER_MODE': 'CUT',
                        'AF_LOADER_PATTERN': '^.+\.xlsx',  # маска поиска файла по имени (regular expression match)
                        'AF_LOADER_STORAGE': loader_storage,
                        'AF_LOGLEVEL': 'DEBUG'
                        }
                     },
        volumes=[volume_source, volume_cache],
        working_dir='/app',
        command='python /app/source/python/run_loader.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='host'
    )
    return t


def create_operator(task_id, task_env, command='python /app/source/python/run.py'):
    return DockerOperator(
        task_id=task_id,
        image='r5_operator:latest',
        api_version='auto',
        auto_remove=True,
        environment={**environment,
                     **task_env
                     },
        volumes=volumes,
        working_dir='/app',
        command=command,
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge'
    )


with DAG(dag_id=dag_id, default_args=default_args, catchup=False, schedule_interval=None) as dag:
    get_data_task = dwash_loader(
        dag=dag,
        name=bucket_name.replace(' ', '_'),
        token=bucket_tokens,
        bucket_name=bucket_name,
        loader_storage=f'{load_directory}'
    )

    convert_data_task = create_operator(
        f'convert_data',
        {'AF_SCRIPT_PATH': f'{python_path}/transform_data.py',
         'AF_LOAD_PATH': f'{load_directory}',
         },
        'python /app/source/python/run.py'
    )

    insert_task = create_operator(
        f'insert_data',
        {'AF_SCRIPT_PATH': f'{python_path}/insert_data.py'
         },
        'python /app/source/python/run.py'
    )

    get_data_task >> convert_data_task >> insert_task  # порядок выполнения тасков