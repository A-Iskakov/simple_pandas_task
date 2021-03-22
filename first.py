from csv import DictReader
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
    # print(data[data['tid'] == 190563850])
    #
    # print(data['tid'].min())
    # pd.Interval.overlaps()
    # data['duration'] = pd.IntervalIndex.from_arrays(data['assigned_ts'], data['closed_ts'], closed='both').overlaps(
    #     pd.IntervalIndex.from_arrays(data['assigned_ts'].shift(1), data['closed_ts'].shift(1), closed='both')
    # )
    pd.set_option("min_rows", 20)
    # pd.set_option("max_columns", 100)
    # pd.set_option("precision", 7)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', 25)
    data['prev'] =  data['assigned_ts'] > data['closed_ts'].shift(1)
    data.loc[0, 'prev'] =  True


    # data['duration'] = data['test'].length
    print(data.dtypes)




    # data['task_duration'] =data['closed_ts'] - data['assigned_ts']



    # for a in data.itertuples(index=False):
    #     print(a.tid)
    #     a.tid1= 'asd'
    #     # print(b)
    #     # print(pd.Interval(row[4], row[5] ))
    #     break
    # for a,b in data.iterrows():
    #     # print(b.tid)
    #     b.tid1= 'asd'
    #     # print(b)
    #     # print(pd.Interval(row[4], row[5] ))
    #     # break

    # for a,b in data.iteritems():
    #     if a != 'closed_ts':
    #         continue
    #
    #     for line, some in enumerate(b):
    #         print(some)
    #         data._set_value(line, 'asdasd', 'asdasdasdas')
    #         break
    #     break
        # print(pd.Interval(row[4], row[5] ))
        # break
    # print(data)
    # data['task_duration'] = data['tid'] +5
    # print(data[data['prev'] == True].sort_values('assigned_ts',
    #                                              # ascending=False
    #                                              ))
    # data['test'] = data.shift(-1).index == 2
    data['texs'] = data.index
    # copy_data = data.copy(deep=True)

    # print(data[(data['prev'] == True) & (data.index > 9)].first_valid_index())
    # data['test'] = (data['prev'].shift(-1) == True )& (data[data['prev'] == True].first_valid_index())
    # print(copy_data[(copy_data['prev'] == True) & (copy_data.index <11)].first_valid_index())


    # data.loc[data['prev'].shift(-1) == True, 'test'] = data['texs']

    # print(data['texs'].sub(data['Microtasks']))

    # print(data['tid'].sub(190560246).abs().idxmin())
    # print(data.loc[(data.shift(1)['texs'] < data['texs']) & (data['prev'] == True), 'texs'].iloc[0])
    # data.loc[data['prev'].shift(-1) == True, 'test'] = data[data.loc[data['tid'].sub(190565906).abs().idxmin(), 'Microtasks'] >3, 'Microtasks']


    # data.loc[data['prev'].shift(-1) == True, 'test'] = data[(data['prev'] == True) & (data.index >data.index)].first_valid_index()
    # https://stackoverflow.com/questions/39575947/get-next-value-from-a-row-that-satisfies-a-condition-in-pandas
    def are(x):
        print(x)
        exit()
    data[data['prev'] == True].groupby(['prev']).apply(lambda x: are(x))
    exit()
    pbar = tqdm.tqdm(total= data.loc[data['prev'] == True].size)
    def get_sublist(x):
        """

        :type x: pd.Series
        """
        pbar.update(1)
        # print(x)
        # print()
        # print(x.texs, end='\r', flush=True)
        try:
            return data.loc[(data['texs'] > x.texs) & (data['prev'] == True), 'texs'].iloc[0]
        except:
            return 0



    data.loc[data['prev'] == True, 'new'] = data[data['prev'] == True].apply(lambda x: get_sublist(x), axis=1)
    print(data)
    # print(data[data['prev'] == True].first_valid_index())



