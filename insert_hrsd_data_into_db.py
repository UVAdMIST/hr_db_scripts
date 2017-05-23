import numpy as np
import pandas as pd
import os
from main_db_script import get_id, append_non_duplicates, make_date_index
from data_utils import hampel_filter, account_for_elev

current_directory = os.path.dirname(__file__)
data_dir = os.path.join(current_directory, '../HRSD')


def qc_shallow_well_data(df):
    column_name = 'Shallow Well_NAVD 88 (ft)'
    df = account_for_elev(df, elev_threshold=7.5, col=column_name)
    df = hampel_filter(df, column_name, 30, threshold=3)
    df['Time Stamp'] = pd.to_datetime(df.loc[:, 'Time Stamp'], format="%m/%d/%Y %H:%M:%S")
    df.set_index('Time Stamp', inplace=True, drop=True)
    df = df.resample('H', how=np.average)
    df.reset_index(inplace=True)
    return df


def qc_wind(df):
    df = df[df.loc[:, ' data flag']<129]
    del df[' data flag']
    return df


def get_file_list(dty, site_nums=None):
    data_files = []
    if site_nums:
        for site_num in site_nums:
            for dirpath, dirnames, filenames in os.walk(dty):
                for filename in filenames:
                    if filename.startswith('MMPS-{}'.format(site_num)) and filename.endswith('.csv'):
                        data_files.append(filename)
    else:
        for dirpath, dirnames, filenames in os.walk(dty):
            for filename in filenames:
                if filename.startswith('MMPS-') and filename.endswith('.csv'):
                    data_files.append(filename)
    return data_files

data_files = get_file_list(dty="{}/to_insert/".format(data_dir))
site_info_table = pd.read_csv("{}/site_info.csv".format(data_dir))
variable_info_table = pd.read_csv("{}/variable_info.csv".format(data_dir))
for data_file in data_files:
    print "inserting data for {}".format(data_file)
    df = pd.read_csv("{}/to_insert/{}".format(data_dir, data_file), index_col=0,
                     parse_dates=True, infer_datetime_format=True)

    site_code = data_file.split('_')[0]
    try:
        site_info = site_info_table[site_info_table.SiteCode == site_code].to_dict('records')[0]
    except IndexError:
        "I don't know this site. Try adding it to the 'site_info.csv' file"
    site_id = get_id('Site', site_info)
    var_column = df.columns.str.strip()[0]

    if var_column.startswith('Rain'):
        variable_code = 'rainfall'
    elif var_column.startswith('Shallow Well') or var_column.startswith('Level_NAVD88_ft'):
        variable_code = 'shallow_well_depth'
    elif var_column.startswith('Wind Direction'):
        variable_code = 'WDF2'
        df = qc_wind(df)
    elif var_column.startswith('Wind Speed'):
        variable_code = 'WSF2'
        df = qc_wind(df)
    else:
        raise ValueError('I do not now what variable you are trying to insert')
    
    variable_info = variable_info_table[
        variable_info_table.VariableCode == variable_code].to_dict('records')[0]
    variable_id = get_id('Variable', variable_info)
    df.columns = ['Value']
    df.index.rename('Datetime', inplace=True)
    df['VariableID'] = variable_id
    df['SiteID'] = site_id
    if 'wind' in variable_info['VariableName'].lower():
        df['QCID'] = 2
    else:
        df['QCID'] = 0
    append_non_duplicates('datavalues', df, ['SiteID', 'Datetime', 'VariableID'], site_id=site_id)
