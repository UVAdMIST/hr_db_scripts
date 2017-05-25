import pandas as pd
import requests
import bs4
from main_db_script import get_id, append_non_duplicates, make_date_index
from io import StringIO


def get_server_data(beg_date, end_date, station_num, var_type, units, fmt='xml'):
    print("fetching {} data for station:{}  for {} through {}".format(var_type, station_num,
                                                                  beg_date,
                                                                  end_date))
    url = get_request_url(beg_date, end_date, station_num, var_type, units, fmt)
    response = requests.get(url)
    soup = bs4.BeautifulSoup(response.text, 'lxml')
    return soup


def get_request_url(beg_date, end_date, station_num, var_type, units, fmt='xml'):
    url = "http://tidesandcurrents.noaa.gov/api/datagetter?begin_date={}&" \
          "end_date={}&station={}&product={}&datum=MSL&units={}&" \
          "time_zone=lst&application=web_services&format={}".format(
        beg_date, end_date, station_num, var_type, units, fmt
    )
    return url


def parse_tide_data(soup, data_tag, val_tag, site_id, variable_id):
    datapoints = soup.find_all(data_tag)
    res_list = []
    if len(datapoints) > 0:
        for d in datapoints:
            v = d[val_tag]
            if v.strip() != '':
                time = d['t']
                res = {
                    'VariableID': variable_id,
                    'SiteID': site_id,
                    'Value': float(v),
                    'Datetime': time
                }
                res_list.append(res)
        df = pd.DataFrame(res_list)
        df = make_date_index(df, 'Datetime')
        append_non_duplicates('datavalues', df, ['SiteID', 'Datetime', 'VariableID'],
                              site_id=site_id, var_id=variable_id)
        return df
    else:
        print "there was no data for this time period"


def get_variable_data(variable_code, units='english'):
    if variable_code == 'hourly_height' and units == 'english':
        var_info = {
            'VariableCode': variable_code,
            'VariableName': 'tide level',
            'Units': 'ft'
        }
    elif variable_code == 'g':
        var_info = {
            'VariableCode': 'WGF6',
        }
    elif variable_code == 'd':
        var_info = {
            'VariableCode': 'WDF6',
        }
    elif variable_code == 's':
        var_info = {
            'VariableCode': 'WSF6',
        }
    elif variable_code == 'h':
        var_info = {
            'VariableCode': 'high_tide',
        }
    elif variable_code == 'l':
        var_info = {
            'VariableCode': 'low_tide',
        }
    elif variable_code == 'hh':
        var_info = {
            'VariableCode': 'high_high_tide',
        }
    elif variable_code == 'll':
        var_info = {
            'VariableCode': 'low_low_tide',
        }
    else:
        raise Exception('we do not have info for this tide variable code')
    return var_info


def get_tide_site_data(soup, src_org):
    site_code = soup.find('metadata')['id']
    site_name = soup.find('metadata')['name']
    site_lat = soup.find('metadata')['lat']
    site_lon = soup.find('metadata')['lon']
    return {'SiteCode': site_code,
            'SiteName': site_name,
            'SourceOrg': src_org,
            'Lat': site_lat,
            'Lon': site_lon}


def update_data(dates, var_type, station_num, data_tag, value_tag, units, var_id):
    """
    
    :param dates: list of tuples [(start_date0, end_date0), (start_date1, end_date1),...]
    :param var_type: string to be tranlated into var_infor from get_variable_data function 
    :param station_num: int, station number
    :param data_tag: the tag that contains all the data for hourly height ('hr') for wind ('ws') 
    :param value_tag: the xml tag that actually contains the data  
    :return: 
    """
    soup = get_server_data(dates[0][0], dates[0][1], station_num, var_type, units)
    site_info = get_tide_site_data(soup, 'NOAA')
    site_id = get_id('Site', site_info)
    for d in dates:
        soup = get_server_data(d[0], d[1], station_num, var_type, units)
        parse_tide_data(soup, data_tag, value_tag, site_id, var_id)


def update_tide_data(yrs, station_num, var_type, units):
    if var_type == 'hourly_height':
        data_tag = "hr"
    else:
        raise Exception('we do not know the data tag for this variable code')
    variable_info = get_variable_data(var_type, units)
    var_id = get_id('Variable', variable_info)
    value_tag = 'v'
    dates = get_yearly_dates(yrs)
    update_data(dates, 'hourly_height', station_num, data_tag, value_tag, units, var_id)


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
    var_types = ['s', 'g', 'd']
    dates = get_monthly_dates(yrs)
    for v in var_types:
        variable_info = get_variable_data(v, units)
        var_id = get_id('Variable', variable_info)
        update_data(dates, 'wind', station_num, 'ws', v, units, var_id)


def update_dly_hi_lo(yrs, station_number, units):
    dates = get_yearly_dates(yrs)
    df_list = []
    for d in dates:
        url = get_request_url(d[0], d[1], station_number, 'high_low', units, fmt='csv')
        r = requests.get(url)
        data = StringIO(r.text)
        df = pd.read_csv(data, index_col='Date Time', infer_datetime_format=True, parse_dates=True)
        df['TY'] = df['TY'].str.strip().str.lower()
        df = df.pivot(columns='TY', values=' Water Level')
        df_list.append(df)
    df_combined = pd.concat(df_list)
    site_id = get_id('Site', {'SiteCode': station_number})
    for v in df_combined.columns:
        variable_id = get_id('Variable', get_variable_data(v))
        qc_id = 2
        idf = pd.DataFrame(df_combined[df_combined[v].notnull()][v])
        idf.index.rename('Datetime', inplace=True)
        idf.rename(columns={v: 'Value'}, inplace=True)
        idf['SiteID'] = site_id
        idf['VariableID'] = variable_id
        idf['QCID'] = qc_id
        print 'inserting {} values at site {}'.format(v, station_number)
        append_non_duplicates('datavalues', idf, ['SiteID', 'VariableID', 'Datetime', 'Value'],
                              site_id=site_id, var_id=variable_id)

st_year = 2010
e_year = 2017
years = range(st_year, e_year)
# 8638610 - sewell's point station
# 8639348 - money point station

station = '8639348'
units = 'english'
var_type = 'wind'
# update_dly_hi_lo(years, station, units)
update_wind_data(years, station, units)