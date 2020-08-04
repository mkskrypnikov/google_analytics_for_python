#!/usr/bin/env python
# coding: utf-8
print('получение данных из GA')

#!pip install --upgrade google-api-python-client
#!pip install oauth2client


'''
#Блок авторизации
print('авторизация')
import argparse
from apiclient.discovery import build
import httplib2
from oauth2client import client
from oauth2client import file
from oauth2client import tools
scope = ['https://www.googleapis.com/auth/analytics.readonly']
api_name = 'analytics'
api_version = 'v3'
client_secrets_path = 'client_secret.json'
parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter, parents=[tools.argparser])
flags = parser.parse_args([])
flow = client.flow_from_clientsecrets(client_secrets_path, scope=scope, message=tools.message_if_missing(client_secrets_path))
storage = file.Storage(api_name + '.dat')
credentials = storage.get()
credentials = tools.run_flow(flow, storage, flags)

'''


# Обновление токена
print('обновление токена')
import json
import requests
from datetime import datetime, timedelta
from pprint import pprint
config = json.load( open('analytics.dat') )
client_id = config['client_id']
client_secret = config['client_secret']
refresh_token = config['refresh_token']
def update_token(client_id, client_secret, refresh_token):
    """Обновление токена для запросов к API. Возвращает токен"""    
    url_token = 'https://accounts.google.com/o/oauth2/token'
    params = { 'client_id' : client_id, 'client_secret' : client_secret, 
               'refresh_token' : refresh_token, 'grant_type' : 'refresh_token' }
    r = requests.post( url_token, data = params )  
    print('Токен выдан до {}'.format(datetime.today() + timedelta( hours = 1 )))
    return r.json()['access_token']
token = update_token(client_id, client_secret, refresh_token)


# Библиотеки
print('импорт библиотек')
import pandas as pd
import json
import requests
from requests.exceptions import ConnectionError
from time import sleep
import json
from datetime import datetime as dt
from datetime import date, timedelta
import time
import datetime
import numpy as np



# Даты для отчета (последние 3 дня)
print('формирование дат')
lastday = datetime.datetime.now()
lastday = lastday - timedelta(days=1)
lastday =  lastday.strftime("%Y-%m-%d")

lastdaya = datetime.datetime.now()
lastdaya = lastdaya - timedelta(days=2) 
lastdaya = lastdaya.strftime("%Y-%m-%d")
lastdayb = datetime.datetime.now()
lastdayb = lastdayb - timedelta(days=3)
lastdayb = lastdayb.strftime("%Y-%m-%d")

#Данные для отчета
print('формирование метрик')
ga_id = 'ххххххххх' #указать свой ID

#Выбор дат
start_dates = '2020-08-01'
end_dates = '2020-08-02'
#start_dates = lastdayb
#end_dates = lastday

metrics_ga = 'ga:sessions,ga:transactions,ga:transactionRevenue,ga:impressions,ga:adCost,ga:adClicks'
dimmensions_ga = 'ga:date,ga:sourceMedium,ga:campaign,ga:deviceCategory'
filters_ga = 'ga:medium=~cpc|direct|adwords|not;ga:source=~yandex|google'
strtindex_ga = 1
maxresults_ga='10000'
start_date = dt.strptime(start_dates, '%Y-%m-%d')
end_date = dt.strptime(end_dates, '%Y-%m-%d')
keyforga = 'https://www.googleapis.com/analytics/v3/data/ga?ids=ga:'+ ga_id+'&start-date='+start_date.strftime('%Y-%m-%d')+'&end-date='+start_date.strftime('%Y-%m-%d')+'&metrics='+metrics_ga+'&dimensions='+dimmensions_ga +'&filters='+filters_ga+'&max-results='+maxresults_ga+'&start-index='+str(strtindex_ga)+'&access_token='+token


# Запрос
print('получение данных')
start_time = dt.now()
delta = timedelta(days=1)
td = dt.date(start_date)
df_new = pd.DataFrame() 

while start_date <= end_date:
    ddf = 0
    strtindex_ga = 1
    keyforga = 'https://www.googleapis.com/analytics/v3/data/ga?ids=ga:'+ ga_id+'&start-date='+start_date.strftime('%Y-%m-%d')+'&end-date='+start_date.strftime('%Y-%m-%d')+'&metrics='+metrics_ga+'&dimensions='+dimmensions_ga +'&filters='+filters_ga+'&max-results='+maxresults_ga+'&start-index='+str(strtindex_ga)+'&access_token='+token
    keyforga = keyforga.replace(':', '%3A')
    keyforga = keyforga.replace(',', '%2C')
    keyforga = keyforga.replace('|', '%7C')
    keyforga = keyforga.replace('!', '%21')
    keyforga = keyforga.replace('=', '%3D')
    keyforga = keyforga.replace(';', '%3B')
    keyforga = keyforga.replace('%3Dga', '=ga')
    keyforga = keyforga.replace('date%3D', 'date=')
    keyforga = keyforga.replace('metrics%3D', 'metrics=')
    keyforga = keyforga.replace('dimensions%3D', 'dimensions=')
    keyforga = keyforga.replace('metrics%3D', 'metrics=')
    keyforga = keyforga.replace('filters%3D', 'filters=')
    keyforga = keyforga.replace('token%3D', 'token=')
    keyforga = keyforga.replace('index%3D', 'index=')
    keyforga = keyforga.replace('results%3D', 'results=')
    keyforga = keyforga.replace('https%3A', 'https:')
    from getpass import getpass
    api_query_uri = keyforga
    r = requests.get(api_query_uri)
    data= r.json()
    df = pd.DataFrame(data['rows'])
    df = df.rename(columns={0: 'Date', 1: 'SourceMedium', 2: 'CampaignName', 3: 'Device', 4: 'Sessions', 5: 'Transactions', 6: 'Revenue', 7: 'gaimpressions', 8: 'gaCost', 9: 'gaClicks'})
    df['Sessions'] = df['Sessions'].astype(int)
    df['Transactions'] = df['Transactions'].astype(int)
    df['Revenue'] = df['Revenue'].astype(float)
    df['Date'] = pd.to_datetime(df['Date'])
    df_new = df_new.append(df, ignore_index = False)
    ddf = len(df)
    print(start_date.strftime('%Y-%m-%d'))
    print(ddf)
        
    while ddf >= 10000:
        strtindex_ga = int(strtindex_ga)
        strtindex_ga += 10000
        keyforga = 'https://www.googleapis.com/analytics/v3/data/ga?ids=ga:'+ ga_id+'&start-date='+start_date.strftime('%Y-%m-%d')+'&end-date='+start_date.strftime('%Y-%m-%d')+'&metrics='+metrics_ga+'&dimensions='+dimmensions_ga +'&filters='+filters_ga+'&max-results='+maxresults_ga+'&start-index='+str(strtindex_ga)+'&access_token='+token
        keyforga = keyforga.replace(':', '%3A')
        keyforga = keyforga.replace(',', '%2C')
        keyforga = keyforga.replace('|', '%7C')
        keyforga = keyforga.replace('!', '%21')
        keyforga = keyforga.replace('=', '%3D')
        keyforga = keyforga.replace(';', '%3B')
        keyforga = keyforga.replace('%3Dga', '=ga')
        keyforga = keyforga.replace('date%3D', 'date=')
        keyforga = keyforga.replace('metrics%3D', 'metrics=')
        keyforga = keyforga.replace('dimensions%3D', 'dimensions=')
        keyforga = keyforga.replace('metrics%3D', 'metrics=')
        keyforga = keyforga.replace('filters%3D', 'filters=')
        keyforga = keyforga.replace('token%3D', 'token=')
        keyforga = keyforga.replace('index%3D', 'index=')
        keyforga = keyforga.replace('results%3D', 'results=')
        keyforga = keyforga.replace('https%3A', 'https:')
        from getpass import getpass
        api_query_uri = keyforga
        r = requests.get(api_query_uri)
        data= r.json()
        df = pd.DataFrame(data['rows'])
        df = df.rename(columns={0: 'Date', 1: 'SourceMedium', 2: 'CampaignName', 3: 'Device', 4: 'Sessions', 5: 'Transactions', 6: 'Revenue', 7: 'gaimpressions', 8: 'gaCost', 9: 'gaClicks'}) 
        df['Sessions'] = df['Sessions'].astype(int)
        df['Transactions'] = df['Transactions'].astype(int)
        df['Revenue'] = df['Revenue'].astype(float)
        df['Date'] = pd.to_datetime(df['Date'])
        df_new = df_new.append(df, ignore_index = False)
        time.sleep(0.5)
        ddf = len(df)
        print(start_date.strftime('%Y-%m-%d'))
        print(ddf)
    start_date += delta
    time.sleep(0.5)
print(dt.now() - start_time)
df_new.head()


# Предобработка данных

print('предобработка данных')
start_time = dt.now()


df_new['источник'] = np.where(df_new['SourceMedium'].str.contains("oogle", case=False, na=False), 'google', 'yandex')
df_new['поиск/сеть'] = np.where(df_new['CampaignName'].str.contains("srch", case=False, na=False), 'поиск', 
np.where(df_new['CampaignName'].str.contains("-srch-cat-nz-net_", case=False, na=False), 'поиск', 'сеть')) 
df_new['тип'] = np.where(df_new['CampaignName'].str.contains("dsa", case=False, na=False), 'dsa', 
np.where(df_new['CampaignName'].str.contains("-nz", case=False, na=False), 'nz', 
np.where(df_new['CampaignName'].str.contains("_nz", case=False, na=False), 'nz',
np.where(df_new['CampaignName'].str.contains("shop", case=False, na=False), 'shoping',
np.where(df_new['CampaignName'].str.contains("corporate", case=False, na=False), 'b2b', 
np.where(df_new['CampaignName'].str.contains("promo", case=False, na=False), 'акции',
np.where(df_new['CampaignName'].str.contains("brand", case=False, na=False), 'бренд',
np.where(df_new['CampaignName'].str.contains("cat-cv", case=False, na=False), 'кат + вендор',
np.where(df_new['CampaignName'].str.contains("categor", case=False, na=False), 'категории',
np.where(df_new['CampaignName'].str.contains("compet", case=False, na=False), 'конкуренты',
np.where(df_new['CampaignName'].str.contains("config", case=False, na=False), 'конфигуратор',
np.where(df_new['CampaignName'].str.contains("rmkt", case=False, na=False), 'ремарктеинг',
np.where(df_new['CampaignName'].str.contains("usilenie", case=False, na=False), 'усиление', 'разное')))))))))))))
df_new['CampaignName'] = df_new['CampaignName'].replace(to_replace ='_10', value ='', regex =True)
df_new['CampaignName'] = df_new['CampaignName'].replace(to_replace ='_1', value ='', regex =True)
df_new['CampaignName'] = df_new['CampaignName'].replace(to_replace ='_2', value ='', regex =True)
df_new['CampaignName'] = df_new['CampaignName'].replace(to_replace ='_3', value ='', regex =True)
df_new['CampaignName'] = df_new['CampaignName'].replace(to_replace ='_4', value ='', regex =True)
df_new['CampaignName'] = df_new['CampaignName'].replace(to_replace ='_5', value ='', regex =True)
df_new['CampaignName'] = df_new['CampaignName'].replace(to_replace ='_6', value ='', regex =True)
df_new['CampaignName'] = df_new['CampaignName'].replace(to_replace ='_7', value ='', regex =True)
df_new['CampaignName'] = df_new['CampaignName'].replace(to_replace ='_8', value ='', regex =True)
df_new['CampaignName'] = df_new['CampaignName'].replace(to_replace ='_9', value ='', regex =True)
df_new['CampaignName'] = df_new['CampaignName'].replace(to_replace ='_0', value ='', regex =True)
df_new = df_new[df_new.SourceMedium.str.contains('etwork')==False]
print("дата: ", df_new['Date'].min(), "-", df_new['Date'].max())
print("количество строк: ",len(df_new))
print("транзакции", df_new['Transactions'].sum())
print("доход", df_new['Revenue'].sum())
print(dt.now() - start_time)
df_new.head()


#Первичное сохранение данных
print('сохранение в файл')
df_new.to_csv('ga_new.csv', index=False, header=True, sep=';', encoding='utf8')


'''
# Очистка последних 3 дней в отчете
print('очистка исторических данных')
start_time = dt.now()
fix = pd.read_csv('ga_new.csv', sep=';', encoding='utf8', header=0)
fix = fix[fix.Date.str.contains(lastday)==False]
fix = fix[fix.Date.str.contains(lastdaya)==False]
fix = fix[fix.Date.str.contains(lastdayb)==False]
fix.to_csv('ga_new.csv', index=False, header=True, sep=';', encoding='utf8')
print(dt.now() - start_time)
fix.head()


#Перезапись последнийх 3 дней
print('сохранение данных')
start_time = dt.now()
x1 = pd.read_csv('ga_new.csv', sep=';', encoding='utf8', header=0)
x1 = x1.append(df_new, ignore_index=False)
x1.to_csv('ga_new.csv', index=False, header=True, sep=';', encoding='utf8')
print(dt.now() - start_time)
'''




