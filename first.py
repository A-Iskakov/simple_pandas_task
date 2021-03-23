"""
Имеется файл с временной статистикой работы асессоров над однотипным заданием.

Формат файла: login tid Microtasks assigned_ts tclosed_ts.

Пояснение к формату: login — логин асессора; tid — id оцениваемого задания (task id); Microtasks – количество микрозаданий в одном задании; assigned_ts — время резервирования системой задания для асессора; closed_ts — точное время завершения работы над заданием; разделитель — табуляция \t.

Задание может состоять из одного или несколько микрозаданий. Время резервирования задания (assigned_ts) указывает на тот момент, когда система назначила определенного асессора исполнителем этого задания. Этот момент может совпадать с временем начала работы асессора над заданием, а может и не совпадать (асессор может отойти выпить чаю, а потом приступить к заданию, асессор может выполнять предыдущее задание, в то время как за ним зарезервированы новые).

Предположим, что асессор за 30 секунд своего рабочего времени получает N рублей.

Какую оплату вы считаете справедливой для выполнения асессором одного микрозадания из этого файла? Опишите подробно все этапы вашего решения.
"""

import numpy as np
import pandas as pd

import tqdm as tqdm

from pandas import ExcelWriter

# открываем файл задания
with open('data_task4_old.txt', 'r', ) as csv_file:
    data = pd.read_csv(csv_file,
                       sep='	',

                       # указываем тип столбцов
                       dtype={
                           'login': str,
                           'tid': np.int64,
                           'Microtasks': np.int8,
                       },

                       # отдельно указываем даты
                       parse_dates=['assigned_ts', 'closed_ts'],
                       infer_datetime_format=True
                       )

    pd.set_option("min_rows", 25)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', 25)

    # создаем датафрейм для итоговых результатов
    final_result_dataframe = pd.DataFrame(columns=[
        "login", "active_time", "time_per_one_microtask", 'payment_per_one_microtask'
    ])

    # группируем вводную таблицу по номеру логина
    for i, (login, group) in tqdm.tqdm(enumerate(data.groupby('login'))):
        # сортируем все задания в группе по времени начала
        group.sort_values(by='assigned_ts', inplace=True)

        # сбрасываем исходные индексы в группе
        group.reset_index(inplace=True)

        # получаем метки для выявления цепочек непрерывных заданий исходя из того что текущее задание началось после
        # окончания предыдущего
        group['is_start_of_new_sequence'] = group['assigned_ts'] > group['closed_ts'].shift(1)

        # из-за сдвига отдельно отмечаем первую строчку
        group.loc[0, 'is_start_of_new_sequence'] = True

        # для каждого задания, которое является началом цепочки непрерывных заданий
        # выявляем время окончания данной цепочки
        group.loc[group['is_start_of_new_sequence'] == True, 'sequence_closed_ts'] = group.loc[
            group['is_start_of_new_sequence'].shift(-1) == True, 'closed_ts'].append(group.tail(1)['closed_ts']).values

        # на основе времени начала и окончания цепочки выявляем общую продолжительность работы
        active_time = (group.loc[group['is_start_of_new_sequence'] == True, 'sequence_closed_ts'] - group.loc[
            group['is_start_of_new_sequence'] == True, 'assigned_ts']).sum()

        # находим сумму микрозаданий по пользователю
        microtask_count = group['Microtasks'].sum()

        # находим среднюю продолжительно выполнения микрозадания
        time_per_one_microtask = active_time / microtask_count

        # исходя из того, что асессор за 30 секунд своего рабочего времени получает N рублей.
        # вычисляем оплату за выполнение асессором одного микрозадания
        payment_per_one_microtask = f'{np.round(time_per_one_microtask / pd.Timedelta(seconds=30), 2)}N RUR'

        # записываем финальный результат в датафрейм
        final_result_dataframe.loc[i] = [login, active_time, time_per_one_microtask, payment_per_one_microtask]

    # приводим время в читаемый формат перед экспортом
    final_result_dataframe['active_time'] = final_result_dataframe['active_time'].astype(str)
    final_result_dataframe['time_per_one_microtask'] = final_result_dataframe['time_per_one_microtask'].astype(str)

    # инициализируем экспорт
    writer = ExcelWriter('YandexExport.xlsx',
                         # engine='xlsxwriter',
                         datetime_format='hh:mm:ss.000'
                         )

    final_result_dataframe.to_excel(writer, sheet_name='результат')
    writer.save()
    writer.close()
