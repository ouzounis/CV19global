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
import requests
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

# ''' !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! '''
# ''' !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! '''
# ''' !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! '''

def _join_and_process_data():

    # ''' add two more columns in each csv that contains the name of each country and the three digit code of each country '''
    # ''' save the new CSVs to /home/smoutsis/data-V2 '''

    path5 = '/home/smoutsis/data-V'

    path6 = '/home/smoutsis/scripts/files/create/'
    codes = pd.read_csv(path6+'countries_codes_teliko.csv',keep_default_na=False)


    # onlyfiles = ['uv.csv']
    onlyfiles = [ f for f in listdir(path5) if isfile(join(path5,f)) ]
    count = 0
    for csv_v in progressbar(onlyfiles):
    
        count+=1
        if csv_v == 'covid.csv':

            data2 = pd.read_csv(path5+'/'+csv_v, keep_default_na=False)
            data22 = data2.iso_code

            list_2 = []
            list_22 = []
            
            for i in progressbar(range(len(data2))):
                
                flag = 0
                counter = 0
        
                while (flag == 0):

                    if i == 0:
                        if data22[i] == codes.iloc[counter]['country_code_2']:
                            list_2.append(codes.iloc[counter]['country_code_2'])
                            list_22.append(codes.iloc[counter]['country_name'])
                            flag = 1
                        else:
                            counter+=1
                    else:

                        if (data22[i][0:3] == data22[i-1][0:3]):
                            list_2.append(list_2[len(list_2)-1])
                            list_22.append(list_22[len(list_22)-1])
                            flag = 1
                        elif (counter <= (len(codes)-1)) and (data22[i][0:3] != data22[i-1][0:3]):
                            if data22[i] == codes.iloc[counter]['country_code_2']:
                                list_2.append(codes.iloc[counter]['country_code_2'])
                                list_22.append(codes.iloc[counter]['country_name'])
                                flag = 1
                            else:
                                counter+=1
                        else:
                            list_2.append(float("NaN"))
                            list_22.append(float("NaN"))
                            flag = 1

            data2 = data2.assign(country_code_1=list_2)
            data2 = data2.assign(country_name_1=list_22)

            path7 = '/home/smoutsis/data-V2/'
            path72 = path7+'v'+('_')+csv_v
            file = path72
            if(os.path.exists(file) and os.path.isfile(file)):
                os.remove(file)
                print("file deleted")
            else:
                print("file not found")
            data2.to_csv(path72, index = False)

        # ''' the csv for uv are separate for each country, so they are meant in one '''
        # ''' additionally changes the format of the date from 20200101 to 2020-01-01 '''
        elif csv_v == 'uv.csv':

            data3 = pd.read_csv(path5+'/'+csv_v, keep_default_na=False)
            data33 = data3.location_key

            codes2 = pd.read_csv(path6+'UV_codes_teliko.csv',keep_default_na=False)

            list_3 = []
            list_33 = []
            
            for i in progressbar(range(len(data3))):
                
                flag = 0
                counter = 0
        
                while (flag == 0):

                    if i == 0:
                        if data33[i] == codes2.iloc[counter]['country_names']:
                            list_3.append(codes2.iloc[counter]['country_code_1'])
                            list_33.append(codes2.iloc[counter]['country_name_1'])
                            flag = 1
                        else:
                            counter+=1
                    else:

                        if (data33[i] == data33[i-1]):
                            list_3.append(list_3[len(list_3)-1])
                            list_33.append(list_33[len(list_33)-1])
                            flag = 1
                        elif (counter <= (len(codes2)-1)) and (data33[i] != data33[i-1]):
                            if data33[i] == codes2.iloc[counter]['country_names']:
                                list_3.append(codes2.iloc[counter]['country_code_1'])
                                list_33.append(codes2.iloc[counter]['country_name_1'])
                                flag = 1
                            else:
                                counter+=1
                        else:
                            list_3.append(float("NaN"))
                            list_33.append(float("NaN"))
                            flag = 1

            data3 = data3.assign(country_code_1=list_3)
            data3 = data3.assign(country_name_1=list_33)

            for i in progressbar(range(len(data3))):
            
                s = data3.date[i]
                s = str(s)
                data3.date[i] = s[0:4] + '-' + s[4:6] + '-' + s[6:8]

            path7 = '/home/smoutsis/data-V2/'
            path72 = path7+'v'+('_')+csv_v
            file = path72
            if(os.path.exists(file) and os.path.isfile(file)):
                os.remove(file)
                print("file deleted")
            else:
                print("file not found")
            data3.to_csv(path72, index = False)

            print(2)
        else:
        
            data1 = pd.read_csv(path5+'/'+csv_v, keep_default_na=False)
            data12 = data1.location_key
            
            list_1 = []
            list_11 = []

            for i in progressbar(range(len(data1))):
                
                flag = 0
                counter = 0
        
                while (flag == 0):

                    if i == 0:
                        if data12[i] == codes.iloc[counter]['location_key']:
                            list_1.append(codes.iloc[counter]['country_code_2'])
                            list_11.append(codes.iloc[counter]['country_name'])
                            flag = 1
                        else:
                            counter+=1
                    else:

                        if (data12[i][0:2] == data12[i-1][0:2]):
                            list_1.append(list_1[len(list_1)-1])
                            list_11.append(list_11[len(list_11)-1])
                            flag = 1
                        elif (counter <= (len(codes)-1)) and (data12[i][0:2] != data12[i-1][0:2]):
                            if data12[i] == codes.iloc[counter]['location_key']:
                                list_1.append(codes.iloc[counter]['country_code_2'])
                                list_11.append(codes.iloc[counter]['country_name'])
                                flag = 1
                            else:
                                counter+=1
                        else:
                            list_1.append(float("NaN"))
                            list_11.append(float("NaN"))
                            flag = 1
                
            data1 = data1.assign(country_code_1=list_1)
            data1 = data1.assign(country_name_1=list_11)
            path7 = '/home/smoutsis/data-V2/'
            path72 = path7+'v'+('_')+csv_v
            file = path72
            if(os.path.exists(file) and os.path.isfile(file)):
                os.remove(file)
                print("file deleted")
            else:
                print("file not found")
            data1.to_csv(path72, index = False)


# ''' !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! '''
# ''' !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! '''
# ''' !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! '''

def _upload_data_to_postgres():

    ''' upload data from /home/smoutsis/data-V2 to postgres '''
    ''' upload data line by line due to the large size of csv '''

    path7 = '/home/smoutsis/data-V2/'
    path8 = '/home/smoutsis/data-V2/data-V2-temp/'
    onlyfiles2 = [ f for f in listdir(path7) if isfile(join(path7,f)) ]
    # onlyfiles2 = ['v_uv.csv']
    engine = create_engine('postgresql://postgres:hyp@ti@p0stgr3s!@#$@62.217.122.70:5432/postgres')
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



with DAG('data', schedule_interval='@monthly', default_args=default_args, catchup=False) as dag:

    download_data = PythonOperator(
        task_id='download_data',
        python_callable=_download_data
    )

    join_and_process_data = PythonOperator(
        task_id='join_and_process_data',
        python_callable=_join_and_process_data
    )

    upload_data_to_postgres = PythonOperator(
    task_id='upload_data_to_postgres',
    python_callable=_upload_data_to_postgres
    )

    download_data >> join_and_process_data >> upload_data_to_postgres


