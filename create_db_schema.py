import sqlite3
from StringIO import StringIO
from main_db_script import hr_db_filename
import pandas as pd


def add_qc_data(con):
    qc_table = StringIO(
        """0,None,These data are raw values and have not been quality controlled at all
        1,by researcher,These data have been quality controlled by us the researchers using our own judgment or algorithms
        2,by institution,These data have been verified by the institution that provided the data"""
    )
    qc_df = pd.read_csv(qc_table, sep=',', header=-1)
    qc_df.columns = ['QCID', 'name', 'description']
    qc_df.to_sql(con=con, name='qualitycontrollevels', if_exists='append', index=False)

#
# def drop_all_tables(cur, table_list):
#     for table in table_list:
#         try:
#             cur.execute("DROP TABLE {}".format(table))
#         except sqlite3.OperationalError:
#             pass
#
con = sqlite3.connect(hr_db_filename)
c = con.cursor()
#
# tables = ['variables', 'datavalues', 'sites', 'qualitycontrollevels']
# drop_all_tables(c, tables)

# c.execute("""CREATE TABLE variables\
#             (VariableID INTEGER PRIMARY KEY AUTOINCREMENT, \
#             VariableCode text, \
#             VariableName text, \
#             VariableDescription text, \
#             Units text, \
#             TimeSupport text);""")
# c.execute("""CREATE TABLE sites \
#             (SiteID INTEGER PRIMARY KEY AUTOINCREMENT, \
#             SiteCode text, \
#             SiteName text, \
#             SourceOrg text, \
#             Lat real, \
#             Lon real);""")
c.execute("""CREATE TABLE qualitycontrollevels\
            (QCID INTEGER PRIMARY KEY, \
            name text, \
            description text);""")
# c.execute("""CREATE TABLE datavalues \
#             (ValueID INTEGER PRIMARY KEY AUTOINCREMENT, \
#             Value real, \
#             Datetime text, \
#             VariableID, \
#             SiteID,\
#             QCID,\
#             FOREIGN KEY(VariableID) REFERENCES variables(VariableID),\
#             FOREIGN KEY(SiteID) REFERENCES sites(SiteID),\
#             FOREIGN KEY(QCID) REFERENCES qualitycontrollevels(QCID)\
#             );""")
con.commit()
add_qc_data(con)
con.close()
