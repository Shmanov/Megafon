import pandas as pd
from datetime import datetime

#FEATURES_PATH = 'data/features.csv'
#DATA_TEST_PATH = 'data/data_test.csv'
#DATA_TEST_MERGE_PATH = 'data/data_test_merge.csv'

def preparing(path_data, path_features):

    df_features = pd.read_csv(path_features, chunksize=100000, iterator=True, delimiter='\\t')
    df_train = pd.read_csv(path_data, delimiter=',')
    df_train = df_train.drop(columns=['Unnamed: 0'])
    df_train = df_train.drop_duplicates()

    df_features_new = pd.DataFrame()  # пустой фрейм
    for chunk_iter in df_features:  # цикл по кускам dataframe
        filtr_id = chunk_iter['id'].isin(df_train.id)  # создаём фильтр по куску
        chunk = chunk_iter[filtr_id]  # фильтруем записи в куске
        df_features_new = pd.concat([df_features_new, chunk])

    df_features_new['buy_time'] = df_features_new.buy_time.apply(datetime.fromtimestamp)
    df_train['buy_time'] = df_train.buy_time.apply(datetime.fromtimestamp)

    df_features_new = df_features_new.sort_values(by=['buy_time'])
    df_train = df_train.sort_values(by=['buy_time'])

    dt_merge = pd.merge_asof(df_train, df_features_new, on='buy_time', by='id')

    dt_merge = dt_merge.dropna()

    return dt_merge

#data_test = preparing(DATA_TEST_PATH, FEATURES_PATH)
#def save(name = 'data/data_test_prep.csv', path_data, path_features):
#    data_test = preparing(path_data, path_features)
#data_test.to_csv(DATA_TEST_MERGE_PATH, index=False)