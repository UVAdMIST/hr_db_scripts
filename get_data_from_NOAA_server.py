import pandas as pd
import requests
from main_db_script import get_id, append_non_duplicates
from io import StringIO


def get_request_url(beg_date, end_date, station_num, var_type, units='english', fmt='xml'):
    url = "http://tidesandcurrents.noaa.gov/api/datagetter?begin_date={}&" \
          "end_date={}&station={}&product={}&datum=MSL&units={}&" \
          "time_zone=lst_ldt&application=web_services&format={}".format(
        beg_date, end_date, station_num, var_type, units, fmt
    )
    return url


def get_variable_id(noaa_variable_code):
    if noaa_variable_code == 'hourly_height':
        varcode = 'hourly_height'
    elif noaa_variable_code == 'water_level':
        varcode = 'six_min_tide'
    elif noaa_variable_code == 'gust':
        varcode = 'WGF6'
    elif noaa_variable_code == 'direction':
        varcode = 'WDF6'
    elif noaa_variable_code == 'speed':
        varcode = 'WSF6'
    elif noaa_variable_code == 'h':
        varcode = 'high_tide'
    elif noaa_variable_code == 'l':
        varcode = 'low_tide'
    elif noaa_variable_code == 'hh':
        varcode = 'high_high_tide'
    elif noaa_variable_code == 'll':
        varcode = 'low_low_tide'
    else:
        raise Exception('we do not have info for this tide variable code')
    varid = get_id('Variable', {'VariableCode': varcode})
    return varid


def get_yearly_dates(yrs):
    return [('{}0101'.format(d), '{}1231'.format(d)) for d in yrs]


def get_monthly_dates(yrs, max_days_allowed=31):
    start_year = yrs[0]
    end_year = yrs[-1]
    dr = pd.date_range(start='{}-01-01'.format(start_year),
                       end='{}-12-31'.format(end_year),
                       freq='{}D'.format(max_days_allowed))
    num_per = len(dr)
    dr = pd.date_range(start='{}-01-01'.format(start_year),
                       freq='{}D'.format(max_days_allowed),
                       periods=num_per
                       )
    dr_l = [(dr[i].strftime('%Y%m%d'), dr[i+1].strftime('%Y%m%d')) for i in range(len(dr)-1)]
    return dr_l


def update_wind_data(yrs, station_num, units):
    dates = get_monthly_dates(yrs)
    var_type = 'wind'
    df_combined = get_df_from_server(dates, station_num, var_type)
    for col in df_combined.columns:
        try:
            pd.to_numeric(df_combined[col])
        except ValueError:
            del df_combined[col]
    variables = ['speed', 'direction', 'gust']
    for v in variables:
        prep_and_insert_table(df_combined[v], station_num, v, qcid=0)


def get_df_from_server(dates, station_number, var_type):
    df_list = []
    for d in dates:
        url = get_request_url(d[0], d[1], station_number, var_type, fmt='csv')
        r = requests.get(url)
        if 'Error: No data was found.' in r.text:
            continue
        else:
            data = StringIO(r.text)
            df = pd.read_csv(data, index_col='Date Time', infer_datetime_format=True,
                             parse_dates=True)
            df_list.append(df)
    if len(df_list) > 0:
        df_combined = pd.concat(df_list)
        cln_cols = [a.strip() for a in df_combined.columns]
        cln_cols = [a.lower() for a in cln_cols]
        df_combined.columns = cln_cols
        return df_combined
    else:
        return 'There was no data of type {} for station {} for these dates'.format(
            var_type, station_number)


def prep_and_insert_table(series, sitecode, var_type, qcid=0):
    series.rename('Value', inplace=True)
    df = pd.DataFrame(series)
    df.index.rename('Datetime', inplace=True)
    siteid = get_id('Site', {'SiteCode': sitecode})
    df['SiteID'] = siteid 
    varid = get_variable_id(var_type)
    df['VariableID'] = varid
    df['QCID'] = qcid
    append_non_duplicates('datavalues', df, ['SiteID', 'VariableID', 'Datetime', 'Value'],
                          site_id=siteid, var_id=varid)


def update_dly_hi_lo(yrs, station_number):
    dates = get_yearly_dates(yrs)
    df_combined = get_df_from_server(dates, station_number, 'high_low')
    df_combined['TY'] = df_combined['TY'].str.strip().str.lower()
    df_combined = df_combined.pivot(columns='TY', values='water level')
    for v in df_combined.columns:
        prep_and_insert_table(df_combined[v], station_number, v, qcid=2)


def update_water_level_values(yrs, station_number):
    dates = get_monthly_dates(yrs)
    var_type = 'water_level'
    df_combined = get_df_from_server(dates, station_number, var_type=var_type)
    prep_and_insert_table(df_combined['water level'], station_number, var_type, qcid=2)


st_year = 2010
e_year = 2017

years = range(st_year, e_year)
# 8638610 - sewell's point station
# 8639348 - money point station

station = '8639348'
units = 'english'
var_type = 'wind'
# update_dly_hi_lo(years, station, units)
update_water_level_values(years, station)
# update_wind_data(years, station, units)