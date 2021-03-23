# https://yandex.ru/jobs/vacancies/proj_man/data_techman/

import numpy as np
import pandas as pd

import tqdm as tqdm

# открываем файл задания
with open('data_task3.csv', 'r', ) as csv_file:
    data = pd.read_csv(csv_file,
                       sep='	',

                       # указываем тип столбцов
                       dtype={
                           'login': str,
                           'uid': np.int16,
                           'docid': np.int32,
                           'cjud': bool,
                           #
                           'jud': bool,
                       },

                       )

pd.set_option("min_rows", 25)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 25)

data['is_assessor_right'] = data['cjud'] == data['jud']

for i, (docid, group) in tqdm.tqdm(enumerate(data.groupby('docid'))):
    # print()
    correct_percent = np.round((len(group[group['is_assessor_right'] == True]) / len(group)) * 100, 2)
    print(correct_percent)
    if i > 100:
        break
# print(data.dtypes)
# print(data)
