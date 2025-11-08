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
import requests

from airflow.models import DAG
from airflow.operators.python import PythonOperator



default_args = {
    'start_date': datetime(2021, 10, 24)
    # "retry_delay": timedelta(minutes=5)

}

# a function that returns the first n digits of a number, we don't use this 
def first_n_digits(num, n):
    return num // 10 ** (int(math.log(num, 10)) - n + 1)

def _download_data():
    # 2 3 4 5 71 72 73 84 UTF-8

    url = "https://covid.ourworldindata.org/data/owid-covid-data.csv"
    r = requests.get(url, allow_redirects=True)
    
    open("/home/smoutsis/data-V/covid.csv", 'wb').write(r.content)

def _download_data():
    # 2 3 4 5 71 72 73 84 UTF-8
    file = ("/home/smoutsis/data-V/covid.csv")
    if(os.path.exists(file) and os.path.isfile(file)):
        os.remove(file)
        print("file deleted")
    else:
        print("file not found")
    url = "https://covid.ourworldindata.org/data/owid-covid-data.csv"
    r = requests.get(url, allow_redirects=True)
    open("/home/smoutsis/data-V/covid.csv", 'wb').write(r.content)
    # 3 4 5 71 72 73 UTF-8
    file = ('/home/smoutsis/data-V/vaccinations.csv')
    if(os.path.exists(file) and os.path.isfile(file)):
        os.remove(file)
        print("file deleted")
    else:
        print("file not found")
    url = 'https://storage.googleapis.com/covid19-open-data/v3/vaccinations.csv'
    r = requests.get(url, allow_redirects=True)
    open("/home/smoutsis/data-V/vaccinations.csv", 'wb').write(r.content)


    # 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 80 81 82 83 UTF-8
    file = ('/home/smoutsis/data-V/government_response.csv')
    if(os.path.exists(file) and os.path.isfile(file)):
        os.remove(file)
        print("file deleted")
    else:
        print("file not found")

    url = 'https://storage.googleapis.com/covid19-open-data/v3/oxford-government-response.csv'
    r = requests.get(url, allow_redirects=True)
    open("/home/smoutsis/data-V/government_response.csv", 'wb').write(r.content)

    # 30 31 32 33 34 35 36  UTF-8
    file = ('/home/smoutsis/data-V/weather.csv')
    if(os.path.exists(file) and os.path.isfile(file)):
        os.remove(file)
        print("file deleted")
    else:
        print("file not found")
    url = 'https://storage.googleapis.com/covid19-open-data/v3/weather.csv'
    r = requests.get(url, allow_redirects=True)
    open("/home/smoutsis/data-V/weather.csv", 'wb').write(r.content)

    # 47 95 96 97 98 99 100  UTF-8
    file = ('/home/smoutsis/data-V/mobility.csv')
    if(os.path.exists(file) and os.path.isfile(file)):
        os.remove(file)
        print("file deleted")
    else:
        print("file not found")
    url = 'https://storage.googleapis.com/covid19-open-data/v3/mobility.csv'
    r = requests.get(url, allow_redirects=True)
    open("/home/smoutsis/data-V/mobility.csv", 'wb').write(r.content)


    # 49 50 67 68 69 70  UTF-8
    file = ('/home/smoutsis/data-V/epidimiology.csv')
    if(os.path.exists(file) and os.path.isfile(file)):
        os.remove(file)
        print("file deleted")
    else:
        print("file not found")
    url = 'https://storage.googleapis.com/covid19-open-data/v3/epidemiology.csv'
    r = requests.get(url, allow_redirects=True)
    open("/home/smoutsis/data-V/mobility.csv", 'wb').write(r.content)


    ''' uv files are in dat format and transform them in a csv format '''

    # load the UV territories (it contains longitude, latitude and MSG too )
    path = '/home/smoutsis/scripts/files/use/UV_temis_nl.csv'
    df = pd.read_csv(path, encoding="utf8", keep_default_na=False)
    st = df['station/place name'].tolist()

    for i in range (len(st)):
        
        city_country = st[i].split(',')
        city = city_country[0]
        country = city_country[1].strip()
        
        # make the URL of each territory to download the dat files
        url1 = 'https://d1qb6yzwaaq4he.cloudfront.net/uvradiation/v2.0/overpass/uv_'
        url2 = city
        url3 = '_'
        url4 = country
        url5 = '.dat'
        
        url = url1 + url2 + url3 + url4 + url5
        # print(url)
        
        # save the .dat files of each territory
        path_2 = '/home/smoutsis/data-V/UV-dat/'
        path__2 = 'uv' + url3 + city + url3 + country + '.dat'
        path2 = path_2 + path__2
        file = path2
        if(os.path.exists(file) and os.path.isfile(file)):
            os.remove(file)
            print("file deleted")
        else:
            print("file not found")
        r = requests.get(url, allow_redirects=True)
        open(path2, 'wb').write(r.content)


        
        # read the .DAT file
        # #sed = np.loadtxt(path2, unpack = False)
        data = np.loadtxt(path2, usecols=[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16], dtype=object)
        
        # the columns of the csv
        columns2 = ['date', 'location_key', 'UVIEFerr', 'UVDEF', 'UVDEFerr', 'UVDEC', 'UVDECerr', 'UVDVF', 'UVDVFerr', 'UVDVC', 'UVDVCerr', 'UVDDF', 'UVDDFerr', 'UVDDC', 'UVDDCerr', 'CMF', 'ozone', 'UVIEF'] 
        
        # keep only the values of 2020 and later
        for k in range (len(data)):
            
            if first_n_digits(int(data[0][0]), 4) < 2020 :
                data = np.delete(data, (0), axis=0)
                

        # create one more column to five the location_key 
        data = np.append(data,np.zeros([len(data),1]),1)
        data[:,[1, 17]] = data[:,[17, 1]]
        
        # fill the location_key column
        for k in range (len(data)):
            data[k][1] = url2 + url3 + url4    
        
        # create and save the csv (now we have .csv files and not .dat files)
        df2 = pd.DataFrame(data, columns = columns2)
        path3 = '/home/smoutsis/data-V/UV-csv/uv_' + city + '_' + country + '.csv'
        file = path3
        if(os.path.exists(file) and os.path.isfile(file)):
            os.remove(file)
            print("file deleted")
        else:
            print("file not found")
        df2.to_csv(path3, index = False)

        # create a bigger csv that contains all the territories
        if i == 0:
            df_teliko = df2
        else:
            df_teliko = df_teliko.append(df2)

    # save the final csv 
    path4 = '/home/smoutsis/data-V/' + 'uv.csv'
    file = path4
    if(os.path.exists(file) and os.path.isfile(file)):
        os.remove(file)
        print("file deleted")
    else:
        print("file not found")
    df_teliko.to_csv(path4, index = False)





with DAG('download', schedule_interval='@monthly', default_args=default_args, catchup=False) as dag:

    download_data = PythonOperator(
        task_id='download_data',
        python_callable=_download_data
    )

    download_data