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
# data['some'] = 0
data.loc[data['is_assessor_right'] == True, 'some'] = -1

document_correct_percentage = pd.DataFrame(columns=[
    "docid", "correct_percent"
])
# print(data)


for i, (docid, group) in tqdm.tqdm(enumerate(data.groupby('docid'))):
    # print()
    correct_percent = len(group[group['is_assessor_right'] == True]) / len(group)
    if correct_percent <= 0.2:
        data.loc[(data['is_assessor_right'] == False) & (data['docid'] == docid), 'koefficient_pribavki'] = 0.8

    elif correct_percent <= 0.5:
        data.loc[(data['is_assessor_right'] == False) & (data['docid'] == docid), 'koefficient_pribavki'] = 0.4

user_correct_percentage = pd.DataFrame(columns=[
    "uid", "correct_percent"
])
for i, (uid, group) in tqdm.tqdm(enumerate(data.groupby('uid'))):
    # print()
    # print(group)
    user_correct_percentage.loc[i] = [uid,
                                      (group['is_assessor_right'].sum() + group['koefficient_pribavki'].sum()) / len(
                                          group)]

# print(data.dtypes)
user_correct_percentage.sort_values(by='correct_percent', inplace=True)
print(user_correct_percentage)
