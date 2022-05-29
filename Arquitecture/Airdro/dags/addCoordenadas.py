# imports for my dag
from http import client
from airflow import DAG
import pandas as pd
from datetime import datetime
from minio import Minio
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from geopy.geocoders import Nominatim
from time import sleep

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
dag = DAG("add_coordenadas", 
    start_date=datetime(2022, 1, 1), 
    schedule_interval="@once")

def extract():
    obj = client.get_object('processing',
                            'dados_processing.csv')
    
    # data = obj.read()
    df = pd.read_csv(obj)

    df.to_csv('/tmp/dados_processing.csv', index=False)

def add_coordenadas():
    df = pd.read_csv('/tmp/dados_processing.csv')

    localizador = Nominatim(user_agent='Pedro')

    # cols
    df['Lat'] = 0
    df['Long'] = 0

    # loop for cordinates

    if True:
        for i in range(len(df)):
            local = localizador.geocode(df['Rua'].iloc[i] + ', ' + df['Bairro'].iloc[i] + ', ' + df['Cidade'].iloc[i])

            try:
                df['Lat'].iloc[i] = local.latitude
                df['Long'].iloc[i] = local.longitude

            except:
                df['Lat'].iloc[i] = 0
                df['Long'].iloc[i] = 0

            sleep(0.01)
    
    df.to_csv("/tmp/dados_processing.csv", index=False)
    client.fput_object("processing", "dados_processing.csv", "/tmp/dados_processing.csv")

    
# tarefa de pegar os dados
get = PythonOperator(task_id='load_data',
                    provide_context=True,
                    python_callable=extract,
                    dag=dag
)


# tarefa de add coordenadas
add_coordenadas = PythonOperator(task_id="add_coordenadas",
                        python_callable=add_coordenadas)


get >> add_coordenadas