"""
Created on Thu Jan 10 23:36 2022

@author: Stavros Moutsis
email: smoutsis@csd.auth.gr
"""

# import libraries
import numpy as np
import pandas as pd
from csv import reader
from sqlalchemy import create_engine
from time import sleep
from progressbar import progressbar
import csv
from datetime import datetime, timedelta
import wget
import os
import math
from os import listdir
from os.path import isfile, join
import urllib

from airflow.models import DAG
from airflow.operators.python import PythonOperator



default_args = {
    'start_date': datetime(2021, 10, 24)
    # "retry_delay": timedelta(minutes=5)

}

# a function that returns the first n digits of a number, we don't use this 
def first_n_digits(num, n):
    return num // 10 ** (int(math.log(num, 10)) - n + 1)


def _upload_data_to_postgres():

    ''' upload data from /home/smoutsis/data-V2 to postgres '''
    ''' upload data line by line due to the large size of csv '''

    path7 = '/home/smoutsis/data-V2/'
    path8 = '/home/smoutsis/data-V2/data-V2-temp/'
    # onlyfiles2 = [ f for f in listdir(path7) if isfile(join(path7,f)) ]
    onlyfiles2 = ['v_uv.csv']
    engine = create_engine('postgresql:##[add credentials]')
    for csv_2 in progressbar(onlyfiles2):

    # data = pd.read_csv(path7+csv_2)
    # table_name = csv_2.split('.')
    # data.to_sql(table_name[0], engine, index=True, if_exists='replace')
    
        with open(path7+csv_2, 'r') as read_obj:
            
            csv_reader = reader(read_obj)
            p = 0
            flag = 0
            flag2=0
            counter=0
            for row in progressbar(csv_reader):

                if flag == 0:
                    column = row
                    f = open(path8+csv_2.split('.')[0]+'-2.csv', 'w')
                    writer = csv.writer(f)
                    row.append("my_index")
                    for i in range(len(row)):
                        row[i] = row[i].lower()
                    writer.writerow(row)
                    f.close()
                    df2 = pd.read_csv(path8+csv_2.split('.')[0]+'-2.csv')
                    # df2 = df
                    flag=1

                else:
                    
                    df2_length = len(df2)
                    row.append(p)
                    df2.loc[df2_length] = row
                    p+=1
                    counter+=1
                    if counter > 20000 :
                        print('\n')
                        print(p)
                        split1 = csv_2.split('_')
                        if split1[1] == 'uv.csv':
                            print(df2.location_key[0])
                        elif split1[1] == 'covid.csv':
                            print(df2.iso_code[0])
                        else:
                            print(df2.location_key[0])
                        counter = 0

                    # for the first line of each csv if the table exists delete it (in postgres) else append it   
                    if flag2 == 0:
                        df2.to_sql(csv_2.split('.')[0], engine, index=False, if_exists='replace')
                        flag2 = 1
                    else:
                        df2.to_sql(csv_2.split('.')[0], engine, index=False, if_exists='append')
                    
                    df2 = df2.drop([0, 0])
                    # df2 = pd.read_csv('csv_file.csv')



with DAG('upload', schedule_interval='@monthly', default_args=default_args, catchup=False) as dag:



    upload_data_to_postgres = PythonOperator(
    task_id='upload_data_to_postgres',
    python_callable=_upload_data_to_postgres
    )

    upload_data_to_postgres
