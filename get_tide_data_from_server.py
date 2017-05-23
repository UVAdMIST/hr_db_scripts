import pandas as pd
import requests
import bs4
from main_db_script import get_id, append_non_duplicates, make_date_index


def get_server_data(url):
    response = requests.get(url)
    soup = bs4.BeautifulSoup(response.text, 'lxml')
    return soup


def get_tide_data_from_server(yr, station_num, var_type, units):
    print("fetching data for station:{}      for year: {}".format(station_num, yr))
    url = "http://tidesandcurrents.noaa.gov/api/datagetter?begin_date={0}0101&" \
          "end_date={0}1231&station={1}&product={2}&datum=MSL&units={3}&" \
          "time_zone=lst&application=web_services&format=xml".format(
        yr, station_num, var_type, units
    )
    return get_server_data(url)


def parse_tide_data(soup, data_tag, val_tag, site_id, variable_id):
    datapoints = soup.find_all(data_tag)
    res_list = []
    for d in datapoints:
        v = d[val_tag]
        time = d['t']
        res = {
            'VariableID': variable_id,
            'SiteID': site_id,
            'Value': v,
            'Datetime': time
        }
        res_list.append(res)
    df = pd.DataFrame(res_list)
    df = make_date_index(df, 'Datetime')
    append_non_duplicates('datavalues', df, ['SiteID', 'Datetime', 'VariableID'], site_id=site_id)
    return df


def get_tide_variable_data(variable_code, units):
    if variable_code == 'hourly_height' and units == 'english':
        var_info = {
            'VariableCode': variable_code,
            'VariableName': 'tide level',
            'Units': 'ft'
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


def update_tide_data(yrs, station_num, var_type, units):
    variable_info = get_tide_variable_data(var_type, units)
    var_id = get_id('Variable', variable_info)
    soup = get_tide_data_from_server(yrs[0], station_num, var_type, units)
    site_info = get_tide_site_data(soup, 'NOAA')
    site_id = get_id('Site', site_info)

    if var_type == 'hourly_height':
        data_tag = "hr"
    else:
        raise Exception('we do not know the data tag for this variable code')
    value_tag = 'v'
    for y in yrs:
        soup = get_tide_data_from_server(y, station_num, var_type, units)
        parse_tide_data(soup, data_tag, value_tag, site_id, var_id)


start_year = 2010
end_year = 2017
years = range(start_year, end_year)
station = '8639348'
units = 'english'
var_type = 'hourly_height'
update_tide_data(years, station, var_type, units)
