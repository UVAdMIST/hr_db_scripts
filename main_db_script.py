import requests
import bs4
import sqlite3
import pandas as pd
import os

current_directory = os.path.dirname(__file__)
hr_db_filename = os.path.join(current_directory, '../hampt_rd_data.sqlite')


def get_id(typ, data):
    """
    gets either the siteid or variableid from the db
    :param typ: String. Either "Site" or "Variable"
    :param data: Dict. the site or variable data
    :return: int. id of site or variable
    """
    data_df = pd.DataFrame(data, index=[0])
    code_name = '{}Code'.format(typ)
    table_name = '{}s'.format(typ.lower())
    id_name = '{}ID'.format(typ)
    code = data[code_name]
    check_by = [code_name]
    append_non_duplicates(table_name, data_df, check_by)
    table = get_db_table_as_df(table_name)
    id_row = table[table[code_name] == code]
    id_num = id_row[id_name].values[0]
    return id_num


def append_non_duplicates(table, df, check_col, site_id=None, var_id=None):
    """
    adds values that are not already in the db to the db
    :param table: String. name of table where the values should be added e.g. 'sites'
    :param df: pandas df. a dataframe with the data to be potentially added to the db
    :param check_col: List. the columns that will be used to check for duplicates in db e.g.
    'VariableCode' and 'VariableType' if checking a variable
    :return: pandas df. a dataframe with the non duplicated values
    """
    con = sqlite3.connect(hr_db_filename)
    if table=='datavalues' and site_id and var_id:
        sql = "SELECT * FROM datavalues WHERE SiteID = {} AND VariableID = {}".format(site_id,
                                                                                      var_id)
        db_df = get_db_table_as_df(table, sql)
    else:
        db_df = get_db_table_as_df(table)
    if not db_df.empty:
        if table == 'datavalues':
            df.reset_index(inplace=True)
            db_df.reset_index(inplace=True)
        merged = df.merge(db_df,
                          how='outer',
                          on=check_col,
                          indicator=True)
        non_duplicated = merged[merged._merge == 'left_only']
        filter_cols = [col for col in list(non_duplicated) if "_y" not in col and "_m" not in col]
        non_duplicated = non_duplicated[filter_cols]
        cols_clean = [col.replace('_x', '') for col in list(non_duplicated)]
        non_duplicated.columns = cols_clean
        non_duplicated = non_duplicated[df.columns]
        non_duplicated.to_sql(table, con, if_exists='append', index=False)
        return df
    else:
        index = True if table == 'datavalues' else False
        df.to_sql(table, con, if_exists='append', index=index)
        return df


def get_db_table_as_df(name, sql="""SELECT * FROM {};""", dbfilename=hr_db_filename):
    con = sqlite3.connect(dbfilename)
    sql = sql.format(name)
    if name == 'datavalues':
        date_col = 'Datetime'
    else:
        date_col = None
    df = pd.read_sql(sql, con, parse_dates=date_col)
    if name == 'datavalues':
        df = make_date_index(df, 'Datetime')
    return df


def make_date_index(df, field, fmt=None):
    df.loc[:, field] = pd.to_datetime(df.loc[:, field], format=fmt)
    df.set_index(field, drop=True, inplace=True)
    return df


def get_table_for_variable_code(variable_code, site_id=None):
    var_id = get_id('Variable', {'VariableCode': variable_code})
    table_name = 'datavalues'
    sql = """SELECT * FROM {} WHERE VariableID={};""".format(table_name, var_id)
    df = get_db_table_as_df(table_name, sql=sql)
    df = df.sort_index()
    if site_id:
        df = df[df['SiteID'] == site_id]
    return df


def get_code_from_id(typ, id):
    """

    :param typ: string 'Variable' or 'Site'
    :param id: int
    :return:
    """
    table_name = '{}s'.format(typ.lower())
    table = get_db_table_as_df(table_name)
    code_name = '{}Code'.format(typ)
    id_name = '{}ID'.format(typ)
    return table[table[id_name] == id][code_name].values[0]


def parse_wml2_data(wml2url, src_org):
    """
    parses wml2 data into pandas dataframe and adds the data, including the site and variable, into
    the database if not already in there
    :param wml2url: String. the service response in wml2 format
    :param src_org: String. the organization e.g. "USGS"
    :return: dataframe of the time series
    """
    con = sqlite3.connect(hr_db_filename)
    soup = get_server_data(wml2url)
    res_list = []
    site_data = get_site_data(soup, src_org)
    site_id = get_id('Site', site_data)

    variable_block = soup.find_all('wml2:observationmember')
    for v in variable_block:
        value_tags_list = v.find_all('wml2:point')
        variable_data = get_variable_data(v)
        variable_id = get_id('Variable', variable_data)
        for value_tag in value_tags_list:
            datetime = value_tag.find('wml2:time').text
            val = value_tag.find('wml2:value').text
            res = {'VariableID': variable_id,
                   'SiteID': site_id,
                   'Value': val,
                   'Datetime': datetime,
                   }
            res_list.append(res)
    df = pd.DataFrame(res_list)
    df['Value'] = pd.to_numeric(df['Value'])
    df = make_date_index(df, 'Datetime')
    append_non_duplicates('datavalues', df, ['SiteID', 'Datetime', 'VariableID'])
    return df


def get_site_data(soup, src_org):
    site_code = soup.find('gml:identifier').text
    site_name = soup.find('om:featureofinterest')['xlink:title']
    site_lat = soup.find('gml:pos').text.split(' ')[0]
    site_lon = soup.find('gml:pos').text.split(' ')[1]
    return {'SiteCode': site_code,
            'SiteName': site_name,
            'SourceOrg': src_org,
            'Lat': site_lat,
            'Lon': site_lon
            }


def get_variable_data(soup):
    variable_code = soup.find("om:observedproperty")["xlink:href"].split("=")[1]
    variable_name = soup.find("om:observedproperty")["xlink:title"]
    variable_type = soup.find("om:name")["xlink:title"]
    uom = soup.find("wml2:uom")["xlink:title"]
    return {'VariableCode': variable_code,
            'VariableName': variable_name,
            'VariableType': variable_type,
            'Units': uom
            }


def get_server_data(url):
    response = requests.get(url)
    soup = bs4.BeautifulSoup(response.text, 'lxml')
    return soup
