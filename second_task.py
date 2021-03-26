"""
https://yandex.ru/jobs/vacancies/proj_man/data_techman/

Имеется файл с различными оценками асессоров.

Формат файла: login tuid docid jud cjud.

Пояснение к формату: login — логин асессора; uid — id асессора (user id); docid — id оцениваемого документа (document id); jud — оценка асессора (judgement); cjud — правильная оценка (correct judgement); разделитель — табуляция \t.

Оценки могут принимать значение [0, 1], т.е. задание, которое сделали асессоры, имеет бинарную шкалу.

Используя данные об оценках, установите, какие асессоры хуже всего справились с заданием. На какие показатели вы ориентировались и какие метрики вы использовали для ответа на этот вопрос? Можно ли предложить какие-то новые метрики для подсчета качества асессоров с учетом природы оценок у этого бинарного задания?

Опишите подробно все этапы вашего решения.
"""
import numpy as np
import pandas as pd

import tqdm as tqdm

# открываем файл задания
from pandas import ExcelWriter

with open('data_task3.csv', 'r', ) as csv_file:
    data = pd.read_csv(csv_file,
                       sep='	',

                       # указываем тип столбцов
                       dtype={
                           'login': str,
                           'uid': np.int16,
                           'docid': np.int32,
                           'cjud': bool,
                           'jud': bool,
                       })

pd.set_option("min_rows", 25)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 25)

# для каждого документа узнаем справился ли асессор с его оценкой
data['is_assessor_right'] = data['cjud'] == data['jud']


# группируем вводную таблицу по id документа
for i, (docid, group) in tqdm.tqdm(enumerate(data.groupby('docid')), desc='оценка сложности каждого задания'):

    # вычисляем процент успешных попыток оценки данного документа
    correct_percent = len(group[group['is_assessor_right'] == True]) / len(group)

    # если с документом справились менее 20% ассессоров,
    # то его вклад ухудшение показателя качества ассессора будет уменьшен на 0.8 для тех кто не справился с ним
    if correct_percent <= 0.2:
        data.loc[(data['is_assessor_right'] == False) & (data['docid'] == docid), 'doc_complexity_factor'] = 0.8

    # если с документом справились менее 50% ассессоров,
    # то его вклад ухудшение показателя качества ассессора будет уменьшен на 0.4 для тех кто не справился с ним
    elif correct_percent <= 0.5:
        data.loc[(data['is_assessor_right'] == False) & (data['docid'] == docid), 'doc_complexity_factor'] = 0.4


# создаем датафрейм для оценки качества работы асессоров
user_correct_percentage = pd.DataFrame(columns=[
    "uid", "correct_percent"
])

# группируем вводную таблицу по uid асессора
for i, (uid, group) in tqdm.tqdm(enumerate(data.groupby('uid')), desc='оценка качества работы каждого асессора'):

    # подсчитываем в зависимости по формуле
    # (сумма_всех_правильных_оценок + сумма_корректировок_по_сложности_документа) / общее_количество оценненных документов
    user_correct_percentage.loc[i] = [uid,
                                      (group['is_assessor_right'].sum() + group['doc_complexity_factor'].sum()) / len(
                                          group)]


# сортируем по оценке качества работы
user_correct_percentage.sort_values(by='correct_percent', inplace=True)

# инициализируем экспорт
writer = ExcelWriter('Yandex_Task2_Result.xlsx')
user_correct_percentage.to_excel(writer, sheet_name='результат')
writer.save()
writer.close()
print('Результат сохранен в Yandex_Task2_Result.xlsx')

"""
В силу бинарности оценок выполнения задания, желательно отдельно ввести показатели сложности документа, что в будущем
смогло бы подтвердить или опровергнуть саму оценку документа исходя из полученных результатов,
а также более справедливо оценивать работу асессоров

"""