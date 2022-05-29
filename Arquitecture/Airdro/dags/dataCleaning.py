# imports for my dag
from email.errors import NonPrintableDefect
from http import client
from operator import index
from airflow import DAG
import pandas as pd
from datetime import datetime
from minio import Minio
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# variables to conect in minio
dl_server = Variable.get("data_lake_server")
dl_login = Variable.get("data_lake_login")
dl_password = Variable.get("data_lake_password")

# make conection
client = Minio(dl_server,
                access_key=dl_login,
                secret_key=dl_password,
                secure=False)


# load data from minio
def extract():
    obj = client.get_object('landing',
                            'raw_data_cwb.csv')
    
    # data = obj.read()
    df = pd.read_csv(obj)

    df.to_csv('/tmp/dados.csv', index=False)

# clean data
def clean_data():
    # read
    df = pd.read_csv('/tmp/dados.csv')

    # fix C0 "Novo"
    df['C0'] = df['C0'].apply(lambda x: 0 if(x == 'vazio') else 1)

    # fix C1 "Tipo"

    df['C1'] = df['C1'].apply(lambda x: 'Desconhecido' if (x == 'vazio') else x)

    # fix C2 "TipoAnuncio"
    df['C2'] = df['C2'].apply(lambda x: x.strip())
    df['C2'] = df['C2'].apply(lambda x: 'Nenhum' if (x=='vazio') else x)

    # fix C3 "Rua"
    df['C3'] = df['C3'].apply(lambda x: 'Desconhecida' if (x=='vazio') else x)

    # fix C4 "Bairro" e "Cidade"

    df['Bairro'] = df['C4'].apply(lambda x: 'Desconhecido' if (x ==  'vazio') else x.split(',')[0].strip())
    df['Cidade'] = df['C4'].apply(lambda x: 'Desconhecido' if (x ==  'vazio') else x.split(',')[1].strip())

    # fix C5 "Metragem"
    df['C5'] = df['C5'].apply(lambda x: 0 if (x == 'vazio') else x.strip().replace('m²', ''))
    df['C5'] = df['C5'].apply(int)

    # fix C6 "Comodos"
    df['C6'] = df['C6'].apply(lambda x: 0 if (x == 'vazio') else x.replace('dorms', ''))
    df['C6'] = df['C6'].apply(int)


    # fix c7 "Aluguel"
    df['C7'] = df['C7'].apply(lambda x: '0' if (x=='vazio') else x.split('R$')[1])
    df['C7'] = df['C7'].apply(lambda x: x.strip())
    df['C7'] = df['C7'].apply(lambda x: x.replace('.', ''))
    df['C7'] = df['C7'].apply(int)

    # fix c8 "Aluguel"
    df['C8'] = df['C8'].apply(lambda x: '0' if (x=='vazio') else x.split('R$')[1])
    df['C8'] = df['C8'].apply(lambda x: x.strip())
    df['C8'] = df['C8'].apply(lambda x: x.replace('.', ''))
    df['C8'] = df['C8'].apply(int)

    # rename
    df.rename(columns={'C0': 'Novo',
                        'C1': 'Tipo',
                        'C2': 'TipoAnuncio',
                        'C3': 'Rua',
                        'C5': 'Metragem',
                        'C6': 'Comodos',
                        'C7': 'Aluguel',
                        'C8': 'Total'}, inplace=True)

    # clean df
    df = df.loc[df['Aluguel'] > 0]
    df = df.loc[df['Total'] > 0]
    df = df.loc[df['Comodos'] > 0]
    df = df.loc[df['Metragem'] > 0]

    df = df.loc[df['Bairro'] != 'Desconhecido']
    df = df.loc[df['Cidade'] != 'Desconhecido']

    # drop cols
    df = df.drop('C4', axis=1)

    # upload to minio
    df.to_csv('/tmp/dados_limpos.csv', index=False)
    client.fput_object("processing", "dados_processing.csv", "/tmp/dados_limpos.csv")


# default dag
dag = DAG("limpeza_dados", 
    start_date=datetime(2022, 1, 1), 
    schedule_interval="@once")

# tarefa de pegar os dados
get = PythonOperator(task_id='load_data',
                    provide_context=True,
                    python_callable=extract,
                    dag=dag
)

# tarefa de limpeza dos dados
clean = PythonOperator(task_id="clean_data",
                        python_callable=clean_data)

get >> clean