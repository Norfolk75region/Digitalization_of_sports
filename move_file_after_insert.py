import os, glob
from r5.helpers.general import Logger

log = Logger()


def main(**kwargs):
    for file in glob(os.path.join(kwargs['full_dict_path'], '*.csv')):
        file_name = os.path.basename(file)
        file_name = os.path.splitext(file_name)[0]

        file_xlsx = file_name + '.xlsx'
        file_csv = file_name + '.csv'

        os.replace(os.path.join(kwargs['full_dict_path'], file_xlsx), os.path.join(kwargs['new_path'], file_xlsx))
        os.replace(os.path.join(kwargs['full_dict_path'], file_csv), os.path.join(kwargs['new_path'], file_csv))
        return


full_dict_path = '/app/cache' + os.getenv('AF_LOAD_PATH')
full_new_path = '/app/cache' + os.getenv('AF_REPLACE_PATH')
main(full_dict_path=full_dict_path, new_path=full_new_path)
