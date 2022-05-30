# imports for dag
from email.errors import NonPrintableDefect
from http import client
from operator import index
from airflow import DAG
import pandas as pd
from datetime import datetime
from minio import Minio
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler
from numpy import array

# variables to conect in minio
dl_server = Variable.get("data_lake_server")
dl_login = Variable.get("data_lake_login")
dl_password = Variable.get("data_lake_password")

# make conection
client = Minio(dl_server,
                access_key=dl_login,
                secret_key=dl_password,
                secure=False)

# default dag
dag = DAG("feature_engineering", 
    start_date=datetime(2022, 1, 1), 
    schedule_interval="@once")

def make_kpi():
    obj = client.get_object('processing',
                            'dados_processing.csv')
    
    # data = obj.read()
    df = pd.read_csv(obj)

    # zscores
    df['TicketM.Bairro'] = df[['Total', 'Bairro']].groupby('Bairro').transform('mean')
    df['Std.Bairro'] = df[['Total', 'Bairro']].groupby('Bairro').transform('std')

    df['TicketM.Comodos'] = df[['Total', 'Comodos']].groupby('Comodos').transform('mean')
    df['Std.Comodos'] = df[['Total', 'Comodos']].groupby('Comodos').transform('std')

    df['TicketM.Tipo'] = df[['Total', 'Tipo']].groupby('Tipo').transform('mean')
    df['Std.Tipo'] = df[['Total', 'Tipo']].groupby('Tipo').transform('std')

    df['ZScore.Bairro'] = (df['Total'] - df['TicketM.Bairro']) / df['Std.Bairro']
    df['ZScore.Comodos'] = (df['Total'] - df['TicketM.Comodos']) / df['Std.Comodos']
    df['ZScore.Tipo'] = (df['Total'] - df['TicketM.Tipo']) / df['Std.Tipo']

    df['Zscore'] = (df['ZScore.Bairro'] + df['ZScore.Comodos'] + df['ZScore.Tipo']) / 3

    # features
    x1 = df[['Novo', 'Metragem', 'Comodos']]
    x2 = pd.get_dummies(df[['Tipo', 'TipoAnuncio']])

    # scalling and enconding
    scaler = StandardScaler()
    x1 = pd.concat([x1, x2], axis=1)
    x = scaler.fit_transform(x1)

    # feature
    y = df['Total']

    # train model
    lr = LinearRegression()
    lr.fit(x, y)

    # predict
    df['PredictOfModel'] = lr.predict(x)

    # distance of prediction
    df['%PriceOfCityModel'] = df['Total'] / df['PredictOfModel'] * 100

        # fazer dicionários de bairros
    lista_bairros = df['Bairro'].drop_duplicates().tolist()
    df_bairros = dict((bairro, None) for bairro in lista_bairros)

    # fazer dicionário de daframes por bairro
    passed = []
    for i in range(len(df)):
        if passed.count(df['Bairro'].iloc[i]) < 1:
            dff = df.loc[df['Bairro'] == df['Bairro'].iloc[i]]
            df_bairros[df['Bairro'].iloc[i]] = dff
        else:
            pass

    # fazer dicionário de modelos por bairro
    modelos = dict((bairro, []) for bairro in lista_bairros)

    for key in df_bairros:
        x = df_bairros[key][['Novo', 'Metragem', 'Comodos']]

        # scalling and enconding
        scaler = StandardScaler()
        scaler.fit(x)
        x = scaler.transform(x)

        # feature
        y = df_bairros[key]['Total']

        # train model
        lr = LinearRegression()
        lr.fit(x, y)

        # append model
        modelos[key].append(lr)
        modelos[key].append(scaler)

    # make prediction
    df['PredictOfModelByRegion'] = 0

    for rep in range(len(df)):
        # select model and sccaler
        model = modelos[df['Bairro'].iloc[i]][0]
        standard_scaler = modelos[df['Bairro'].iloc[i]][1]

        # make features
        x = [df[['Novo', 'Metragem', 'Comodos']].iloc[rep].array]
        x = standard_scaler.transform(x)

        df['PredictOfModelByRegion'].iloc[rep] = model.predict(x)


    # distance of prediction
    df['%PriceOfRegioModel'] = df['Total'] / df['PredictOfModelByRegion'] * 100

    # save to curated
    df.to_csv("/tmp/dados_curated.csv", index=False)
    client.fput_object("curated", "dados_curated.csv", "/tmp/dados_curated.csv")


# tarefa de KPI"s
make_feature = PythonOperator(task_id="add_kpi",
                        python_callable=make_kpi,
                        dag=dag)

make_feature