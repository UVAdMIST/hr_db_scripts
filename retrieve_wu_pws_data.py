import requests
from datetime import datetime
import json
import pandas as pd
from pandas.io.json import json_normalize
from main_db_script import append_non_duplicates, get_id

api_key = "8842d598ec26dc90"
state = 'VA'
city = 'Norfolk'


def construct_geolookup_url(date):
    global api_key
    global state
    global city
    url = 'http://api.wunderground.com/api/{}/geolookup/history_{}/q/{}/{}.json'.format(
            api_key, 
            str(date), 
            state,
            city
            )
    return url


def construct_pws_url(pws_id, date):
    global api_key
    url = 'http://api.wunderground.com/api/{}/history_{}/q/pws:{}.json'.format(
            api_key, 
            str(date), 
            pws_id)
    return url


def make_numeric(pwsdf):
    for col in pwsdf.columns:
        try:
            pwsdf[col] = pd.to_numeric(pwsdf[col])
        except ValueError:
            continue
    return pwsdf


def make_date_time_column(pwsdf):
    pwsdf['datetime'] = pwsdf[['date.year', 'date.mon', 'date.mday', 'date.hour',
                               'date.min']].apply(lambda x: datetime(*x), axis=1)
    return pwsdf.set_index('datetime')


def pws_data_to_dataframe(pws_id, dates):
    l = []
    for d in dates:
        r = requests.get(construct_pws_url(pws_id, d)) 
        f = open('../wu/raw_data/{}_{}.json'.format(pws_id, d), "w+")
        f.write(r.text)
        f.close()
        data = json.loads(r.text)
        df = json_normalize(data['history']['observations'])
        l.append(df)
    comb_df = pd.concat(l)
    comb_df = make_numeric(comb_df)
    comb_df = make_date_time_column(comb_df)
    return comb_df


def convert_from_wu_var_to_site_code(wu_var):
    if wu_var == 'precip_totali':
        return 'rainfall_cummulative'
    else:
        raise ValueError('there is no conversion for this wu variable yet')


def insert_pws_data_into_db(pws_id, date_list, wu_variables):
    df = pws_data_to_dataframe(pws_id, date_list)
    siteid = get_id('Site', {'SiteCode': pws_id})
    df['SiteID'] = siteid
    df['QCID'] = 0
    for v in wu_variables:
        varid = get_id('Variable', {'VariableCode': 'rainfall_cummulative'})
        df['VariableID'] = varid
        df['Value'] = df[v]
        insdf = df[['SiteID', 'VariableID', 'QCID', 'Value']]
        insdf.index.rename('Datetime', inplace=True)
        print 'inserting {} data into db for site {}'.format(v, station_id)
        append_non_duplicates('datavalues', insdf, ['SiteID', 'VariableID', 'Datetime', 'Value'],
                              site_id=siteid, var_id=varid)


dates = [20131008, 20131009, 20131010]
station_id = 'KVANORFO2'
variables = ['precip_totali']
insert_pws_data_into_db(station_id, dates, variables)
