import numpy as np
import pandas as pd
# interval_1 = pd.Interval(left=pd.Timestamp('2017-04-20 11:09:39'), right=pd.Timestamp('2017-04-20 13:13:01'))
# interval_2 = pd.Interval(left=pd.Timestamp('2017-04-20 13:13:01'), right=pd.Timestamp('2017-04-20 15:13:01'))
# print(
#     interval_1.overlaps(pd.Interval(1,1))
# )
#
# exit()
import tqdm as tqdm

with open('data_task4_old.txt', 'r', ) as csv_file:
    data = pd.read_csv(csv_file,
                       sep='	',
                       dtype={
                           'login': str,
                           'tid': np.int64,
                           'Microtasks': np.int8,
                           # 'assigned_ts': np.datetime_as_string,
                           # 'closed_ts': np.datetime64

                       },
                       parse_dates=['assigned_ts', 'closed_ts'],
                       infer_datetime_format=True
                       )

    # print(data['tid'].min())
    # pd.Interval.overlaps()
    # data['duration'] = pd.IntervalIndex.from_arrays(data['assigned_ts'], data['closed_ts'], closed='both').overlaps(
    #     pd.IntervalIndex.from_arrays(data['assigned_ts'].shift(1), data['closed_ts'].shift(1), closed='both')
    # )
    pd.set_option("min_rows", 25)
    # pd.set_option("max_columns", 100)
    # pd.set_option("precision", 7)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', 25)

    # data['is_start_of_new_sequence'] =  data['assigned_ts'] > data['closed_ts'].shift(1)
    # data.loc[0, 'is_start_of_new_sequence'] =  True

    print(data.dtypes)

    # data['initial_index'] = data.index
    # https://stackoverflow.com/questions/39575947/get-next-value-from-a-row-that-satisfies-a-condition-in-pandas

    # data.loc[data['is_start_of_new_sequence'] == True, 'next_index'] = data.loc[data['is_start_of_new_sequence'] == True, 'initial_index'].shift(-1)
    # print(data.loc[data['is_start_of_new_sequence'].shift(-1) == True, 'closed_ts'])
    # print(len(data.loc[data['is_start_of_new_sequence'] == True]))
    # pd.Series().append()

    # data.loc[data['is_start_of_new_sequence'] == True, 'sequence_closed_ts'] = data.loc[data['is_start_of_new_sequence'].shift(-1) == True, 'closed_ts'].append(data.tail(1)['closed_ts']).values
    df = pd.DataFrame(columns=["login", "active_time", "time_per_one_microtask", 'payment_per_one_microtask'])
    # i = 0
    for i, (login, group) in enumerate(data.groupby('login')):
        group.sort_values(by='assigned_ts', inplace=True)
        group.reset_index(inplace=True)
        group['is_start_of_new_sequence'] = group['assigned_ts'] > group['closed_ts'].shift(1)
        group.loc[0, 'is_start_of_new_sequence'] = True
        group.loc[group['is_start_of_new_sequence'] == True, 'sequence_closed_ts'] = group.loc[
            group['is_start_of_new_sequence'].shift(-1) == True, 'closed_ts'].append(group.tail(1)['closed_ts']).values
        active_time = (group.loc[group['is_start_of_new_sequence'] == True, 'sequence_closed_ts'] - group.loc[
            group['is_start_of_new_sequence'] == True, 'assigned_ts']).sum()
        microtask_count = group['Microtasks'].sum()
        time_per_one_microtask = active_time / microtask_count
        payment_per_one_microtask = f'{np.round(time_per_one_microtask / pd.Timedelta(seconds=30), 2)}N RUR'
        df.loc[i] = [login, active_time, time_per_one_microtask, payment_per_one_microtask]
        # print(login)
        # print(active_time)
        # print(len(group))
        #
        # print()
        # print(active_time/group['Microtasks'].sum())
        # print(group)
        # i += 1
        if i > 20:
            break

    print(df)

    exit()


    def are(x):
        print(x)
        exit()


    data[data['is_start_of_new_sequence'] == True].groupby(['is_start_of_new_sequence']).apply(lambda x: are(x))
    exit()
    pbar = tqdm.tqdm(total=data.loc[data['is_start_of_new_sequence'] == True].size)


    def get_sublist(x):
        """

        :type x: pd.Series
        """
        pbar.update(1)
        # print(x)
        # print()
        # print(x.texs, end='\r', flush=True)
        try:
            return data.loc[(data['texs'] > x.texs) & (data['is_start_of_new_sequence'] == True), 'texs'].iloc[0]
        except:
            return 0


    data.loc[data['is_start_of_new_sequence'] == True, 'new'] = data[data['is_start_of_new_sequence'] == True].apply(
        lambda x: get_sublist(x), axis=1)

    # print(data[data['is_start_of_new_sequence'] == True].first_valid_index())
