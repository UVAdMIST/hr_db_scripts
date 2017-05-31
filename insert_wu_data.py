import pandas as pd
from main_db_script import append_non_duplicates


# add wind direction data to database
rawdf = pd.read_csv('../wu/norf_data.csv')
rawdf.columns = rawdf.columns.str.strip()
siteid = 19
varid = 6
qcid = 0
df = pd.DataFrame()
df['Datetime'] = rawdf['EST']
df['SiteID'] = siteid
df['VariableID'] = varid
df['QCID'] = qcid
df['Value'] = rawdf['WindDirDegrees']
df.set_index('Datetime', inplace=True)
append_non_duplicates('datavalues', df, ['SiteID', 'Datetime', 'VariableID'], site_id=siteid,
                      var_id=varid)
