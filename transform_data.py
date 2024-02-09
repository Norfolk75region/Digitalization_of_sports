import datetime
import pandas as pd
import re
import glob
import os
from r5.helpers.general import Logger

log = Logger()

def main(full_path):
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
	    "МБУ ДО «СШ «Рингстар»": "7328049578",
        "МБУ СШ \"ЛАДА\" г. Димитровград": "7302042874",
	    'МБУ ДО СШ "ЛАДА" г. Димитровград': "7302042874",
        'МБУ ДО СШ "ЛАДА"': "7302042874",
        "МБУ «СШОР «Юность»": "7328509994",
        "МБУ ДО «СШОР «Юность»": "7328509994",
        "МАУ ДО СШ \"Цементник\"": "7321315125",
        "МБУ ДО\"СШОР \"Волга\"": "7328049592",
        "МБУ ДО «СШ «Старт»": "7328061085",
        "МБУ «СШ «Фаворит»": "7328084283",
	    "МБУ ДО «СШ «Фаворит»": "7328084283",
        "МБУ ДО  «СШ «Фаворит»": "7328084283",
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
    date = re.findall('\d\d\.\d\d\.\d\d', f'{file_name}')
    date = datetime.datetime.strptime(date[0], '%d.%m.%y')
    # Преобразование фрейма
    log.warning(f'FULL PATH={full_path}')
    data_frame = pd.read_csv(full_path, sep=';')
    data_frame = data_frame.iloc[3:39, 2:]  # Забираем строки и столбцы с данными
    log.info(data_frame)
    data_frame.columns = range(data_frame.columns.size)  # Переименовываем название столбцов 0-41
    data_frame.reset_index(drop=True, inplace=True)  # сбрасываем нумерацию строк
    data_frame = data_frame.rename(columns=column_name)  # присваиваем колонкам имена из словаря
    data_frame['INN'] = data_frame['INN'].replace(inn)  # заменяем названия организаций на ИНН
    data_frame = pd.DataFrame(
        (data_frame[series] for series in column_name.values())).T  # заменяем датафрейм согласно словарю с таблицами
    data_frame = data_frame.applymap(lambda cell_value: re.sub(r'\D', '', str(cell_value)))#Заменяем все значения в ячейках через lambda функцию
    data_frame['date'] = date.strftime('%Y-%m-%d')
    #data_frame.to_json('inn.json')  # не обязателен
    data_frame.to_csv(full_path, sep=';')
    return

full_dict_path = '/app/cache'+os.getenv('AF_LOAD_PATH')


for file_name in glob.glob(os.path.join(full_dict_path, '*.csv')):
    log.info(f'TRANSFORM: file_name')
    main(file_name)

