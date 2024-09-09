import pandas as pd
import configs
import datetime
import pandas as pd
import os
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Lock

import snowflake.connector

import datetime



conn = snowflake.connector.connect(
    user=configs.user,
    password=configs.password,
    account=configs.account,
    warehouse=configs.importWarehouse,
    database=configs.db,
    schema=configs.schema,
  #  role=configs.role,
)

#global variable
header_sql = "INSERT INTO ELEPHANT_DB.APPLAYDU_NOT_CERTIFIED.STORE_STATS ( VERSION ,GAME_ID ,EVENT_ID ,CLIENT_TIME ,STORE_DATE ,TOKEN ,D_COUNTRY ,COUNTRY_NAME ,CUSTOM_TRACKING ,KPI_NAME ,SERVER_TIME ,UTM_CAMPAIGN ,UTM_CONTENT ,UTM_MEDIUM ,UTM_SOURCE ,UTM_REDIRECT ,EVENT_COUNT ,UTM_TERM ,USER_ID ) values"

def loadCSV(filename, platform):
    if(platform == 'iOS'):
        return pd.read_csv(filename, skiprows=[0,1,2,3])
    
    return pd.read_csv(filename)

def getValue(input):
    if(input!=input): #nan value
        return 0
    
    value=''
    if(isinstance(input, int) or isinstance(input, float)):
        value = '%d'%input
    else:
        value = input.replace(',','')

    if(value=='' or value=='-' or value=='0'):
        return 0
    return int(value)

def synsCountry(country_name):
    country_name = country_name.replace('Slovakia','Slovakia (Slovak Republic)') #cheat for missiong countries
    country_name = country_name.replace('Russia','Russian Federation') #cheat for missiong countries
    country_name = country_name.replace('Czechia','Czech Republic') #cheat for missiong countries
    country_name = country_name.replace('TÃ¼rkiye','Turkey') #cheat for missiong countries
    country_name = country_name.replace('Türkiye','Turkey') #cheat for missiong countries
    
    return country_name



token = 1
def executeSQL(str_sql):
    global token
    str_sql = str_sql [:-1]

    #print(str_sql)
    text_file = open("insert.sql", "wt")
    n = text_file.write(str_sql)
    text_file.close()

    result = conn.cursor().execute(str_sql)
    print(result.rowcount, "record(s) affected")
    #token = 1
    return header_sql
    
def updateSQL(str_sql,game_id,str_date,country_code,country_name,kpi,value):
    global token
    q = "('%s',%d,393584,'%s','%s',%d,'%s','%s','N/A','%s','%s','N/A','N/A','N/A','N/A','N/A',%s,'N/A','N/A'),"%(
        configs.apd_store_version
        ,game_id
        ,str_date
        ,str_date
        ,token
        ,country_code
        ,country_name
        ,kpi
        ,configs.str_today
        ,value
    )
    if token  ==1:
        str_sql = header_sql + q
    else:
        str_sql = str_sql + q
    token = token +1

    if (token %5000 ==0):
        str_sql=executeSQL(str_sql)
        str_sql = header_sql


    
    return str_sql
   

def f_import_ios_data_withcountry(filename):
    global token

    str_sql = header_sql

    df_result = loadCSV(filename,'iOS')

    # df_result['date_filter'] = pd.to_datetime(df_result['Date'])
    # df_result=df_result[df_result['date_filter'] >= pd.to_datetime('2022-07-01')]
    # df_result=df_result[df_result['date_filter'] < pd.to_datetime('2022-08-01')]

    line = open(filename, "r").readlines()[1]
    source_type = line.split(',')[1].strip()
    metric = df_result.columns[1].replace('Afghanistan ','')
    kpi = source_type + ' ' + metric
    
    
    for count, row in df_result.iterrows():
        store_date = datetime.datetime.strptime(row['Date'], "%m/%d/%y")
        str_date = ("%d-%.2d-%.2d" % (store_date.year, store_date.month, store_date.day))

        undefine_country =0
        for col in df_result.columns[1:]:
            if(col == 'date_filter'):
                continue

            value=getValue(row[col])

            if(value == 0):
                continue

            country_name = col.replace(' ' + metric,'')
            country_name = synsCountry(country_name)

            if country_name not in configs.apd_country:
                undefine_country = undefine_country + int(value)
                continue
            country_code = ''
            country_code = configs.COUNTRY_CODE[country_name]

            str_sql = updateSQL(str_sql,81335,str_date,country_code,country_name,kpi,value)
            

        if(undefine_country > 0):
            str_sql = updateSQL(str_sql,81335,str_date,'N/A','Undefined',kpi,undefine_country)

    
    executeSQL(str_sql)


def push_ios_store_data():

    for filename in configs.apd_ios_store_files:
        print('Uploading data of ' + filename)
        f_import_ios_data_withcountry(filename)


def f_import_gp_data_with_country(df_result):
    #kpi='Google Play search'
    global token
    str_sql = header_sql

    token = 1
    measure = ''
    for count, row in df_result.iterrows():
        store_date = datetime.datetime.strptime(row['Date'], "%b %d, %Y")
        str_date = ("%d-%.2d-%.2d" % (store_date.year, store_date.month, store_date.day))
        defined_country = {'Store listing visitors':0,
                            'Store listing acquisitions':0
                            }
        all_country = {'Store listing visitors':0,
                            'Store listing acquisitions':0
                            }
        
        for col in df_result.columns[1:]:
            
            if(col == 'date_filter'):
                continue
            value = getValue(row[col])
            if(value==0):
                continue

            source_type = col.split(':')[0].strip()
            
            kpi_name = col.split(';')[0].strip()
            measure = kpi_name.split(':')[1].strip()
            country_name = col.split(';')[1].strip()

            if(country_name == 'All countries / regions'):
                all_country[source_type] = int(value)
                continue

            country_name = synsCountry(country_name)
            if country_name in configs.apd_country:
                defined_country[source_type] = defined_country[source_type] + int(value)
            else:
                continue

            country_code = configs.COUNTRY_CODE[country_name]

            str_sql = updateSQL(str_sql,81337,str_date,country_code,country_name,kpi_name,value)

        
        #process for undefine
        if(all_country['Store listing visitors'] - defined_country['Store listing visitors'] > 0):
            str_sql = updateSQL(str_sql,81337,str_date,'N/A','Undefined','Store listing visitors: '+ measure,all_country['Store listing visitors'] - defined_country['Store listing visitors'])
        
            

        if(all_country['Store listing acquisitions'] - defined_country['Store listing acquisitions'] > 0):
            str_sql = updateSQL(str_sql,81337,str_date,'N/A','Undefined','Store listing acquisitions: '+ measure,all_country['Store listing acquisitions'] - defined_country['Store listing acquisitions'])
        
            

    str_sql = str_sql [:-1]

    result = conn.cursor().execute(str_sql)
    print(result.rowcount, "record(s) affected")

def push_googleplay_store_data():
    


    gp_kpis =  ['Google Play search',
                'Third-party referrals',
                'Google Play explore'
                ]
    for kpi in gp_kpis:
        df_raw_by_country = {
            'group 1':pd.DataFrame(),
            'group 2':pd.DataFrame(),
            'group 3':pd.DataFrame(),
        }
        for file in configs.apd_gp_data_files:
            if(file['source_type']==kpi):
                df = pd.read_csv(file['filename'])
                df.drop(list(df.filter(regex = 'rate')), axis = 1, inplace = True)
                df.drop(list(df.filter(regex = 'Notes')), axis = 1, inplace = True)

                df.columns = df.columns.str.replace(':' , ': '+file['source_type']+';')
                if(df_raw_by_country[file['group']].empty):
                    df_raw_by_country[file['group']]=df
                else:
                    df_raw_by_country[file['group']] = df_raw_by_country[file['group']].append(df)
                    
        df_raw_by_country['group 1'].to_csv('gp_store_preprocess1.csv')
        df_raw_by_country['group 2'].to_csv('gp_store_preprocess2.csv')
        df_raw_by_country['group 3'].to_csv('gp_store_preprocess3.csv')


        df_result = pd.concat([df_raw_by_country['group 1'].set_index('Date'),df_raw_by_country['group 2'].set_index('Date')], axis=1, join='inner').reset_index()
        df_result = pd.concat([df_result.set_index('Date'),df_raw_by_country['group 3'].set_index('Date')], axis=1, join='inner').reset_index()


        # df_result['date_filter'] = pd.to_datetime(df_result['Date'], format='%b %d, %Y')
        # df_result=df_result[df_result['date_filter'] >= pd.to_datetime('2022-07-01')]
        # df_result=df_result[df_result['date_filter'] < pd.to_datetime('2022-08-01')]

        df_result=df_result.fillna('N/A')

        df_result.to_csv('gp_store_preprocess.csv')
        f_import_gp_data_with_country(df_result)

def f_import_gp_data(df_result):
    #kpi='Google Play search'
    global token
    str_sql = header_sql

    token = 1
    measure = ''
    for count, row in df_result.iterrows():
        store_date = datetime.datetime.strptime(row['Date'], "%b %d, %Y")
        str_date = ("%d-%.2d-%.2d" % (store_date.year, store_date.month, store_date.day))
 
        
        for col in df_result.columns[1:]:
            
            if(col == 'date_filter'):
                continue
            value = getValue(row[col])
            if(value==0):
                continue

            kpi_name = col
            country_name = 'Undefined'
            country_code = 'N/A'

            str_sql = updateSQL(str_sql,81337,str_date,country_code,country_name,kpi_name,value)

           
            

    str_sql = str_sql [:-1]

    result = conn.cursor().execute(str_sql)
    print(result.rowcount, "record(s) affected")

def push_googleplay_store_data_utm():

    gp_kpi = 'Third-party referrals'
    df_raw = pd.DataFrame()

    for file in configs.apd_gp_utm_data_files:
        df = pd.read_csv(file['filename'])
        df.drop(list(df.filter(regex = 'rate')), axis = 1, inplace = True)
        df.drop(list(df.filter(regex = 'Notes')), axis = 1, inplace = True)

        if(df_raw.empty):
            df_raw=df
        else:
            df_raw = df_raw.append(df)
                    
        df_raw.to_csv('gp_store_preprocess.csv')
     

    f_import_gp_data(df_raw)

#push_ios_store_data()
#push_googleplay_store_data()
#push_googleplay_store_data_utm()
