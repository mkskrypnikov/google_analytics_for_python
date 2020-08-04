## Получение данных из GA

#### необходимые пакеты
необходимо установить:
```
!pip install --upgrade google-api-python-client
!pip install oauth2client
```

#### авторизация в GA:
после первой авторизации стоит закомментировать блок, что бы при запуске скрипта по расписанию не происходила повторная переавторизация в аккаунте google
```python
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
```

#### данные для формирования отчета
необходимо указать ID представления в GA
```python
ga_id = 'ххххххххх'
```
выбор произвольных дат очтета:
```python
#Выбор дат
start_dates = '2020-08-01'
end_dates = '2020-08-02'
```
данные за последние 3 дня (для автоматического запуска)
```python
start_dates = lastdayb
end_dates = lastday
```
#### настройка metrics, dimmensions и filters для отчета
```python
metrics_ga = 'ga:sessions,ga:transactions,ga:transactionRevenue,ga:impressions,ga:adCost,ga:adClicks'
dimmensions_ga = 'ga:date,ga:sourceMedium,ga:campaign,ga:deviceCategory'
filters_ga = 'ga:medium=~cpc|direct|adwords|not;ga:source=~yandex|google'
```

#### предобработка отчета:
добавляем столбцы с условиями:
```python

df_new['источник'] = np.where(df_new['SourceMedium'].str.contains("oogle", case=False, na=False), 'google', 'yandex')
df_new['поиск/сеть'] = np.where(df_new['CampaignName'].str.contains("srch", case=False, na=False), 'поиск', 
np.where(df_new['CampaignName'].str.contains("-srch-cat-nz-net_", case=False, na=False), 'поиск', 'сеть')) 
```
исправляем ошибки в данных
```python
df_new['CampaignName'] = df_new['CampaignName'].replace(to_replace ='_10', value ='', regex =True)
```

#### Сохранение данных отчета
сохраняем данные в файл
```python
df_new.to_csv('ga_new.csv', index=False, header=True, sep=';', encoding='utf8')
```
для дальнейшей перезаписи и дополнения текущего файла рекомендуется закомментировать строку кода выше и раскомментировать:
код будет удалять данные иза последние 3 дня и дополнять их актуальными данными аналогичного периода
```python
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
```

файлы:
* analytics.dat
* client_secret.json

содержат данные для авторизации и обновления api ключа