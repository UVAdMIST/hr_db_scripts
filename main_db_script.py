import sqlite3
import pandas as pd
import os

current_directory = os.path.dirname(__file__)
db_filename = os.path.join(current_directory, '../hampt_rd_data.sqlite')


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


def append_non_duplicates(table, df, check_col, site_id=None):
    """
    adds values that are not already in the db to the db
    :param table: String. name of table where the values should be added e.g. 'sites'
    :param df: pandas df. a dataframe with the data to be potentially added to the db
    :param check_col: List. the columns that will be used to check for duplicates in db e.g.
    'VariableCode' and 'VariableType' if checking a variable
    :return: pandas df. a dataframe with the non duplicated values
    """
    con = sqlite3.connect(db_filename)
    if table=='datavalues' and site_id:
        sql = "SELECT * FROM datavalues WHERE SiteID = {}".format(site_id)
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


def get_db_table_as_df(name, sql="""SELECT * FROM {};"""):
    con = sqlite3.connect(db_filename)
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


def get_table_for_variable(variable_id, site_id=None):
    table_name = 'datavalues'
    sql = """SELECT * FROM {} WHERE VariableID={};""".format(table_name, variable_id)
    df = get_db_table_as_df(table_name, sql=sql)
    df = df.sort_index()
    if site_id:
        df = df[df['SiteID'] == site_id]
    return df
