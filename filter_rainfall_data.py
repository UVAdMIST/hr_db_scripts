# coding: utf-8
import numpy as np
from hr_db_scripts.main_db_script import get_table_for_variable_code, get_db_table_as_df, hr_db_filename
import datetime
import sqlite3

df = get_table_for_variable_code('rainfall')
dfp = df.pivot(columns='SiteID', values='Value')

dfp_med = dfp.median(1)
dfp_max = dfp.max(1)
dfp_std = dfp.std(1)

dfp_nz = dfp[dfp_max>0]
dfp_nzi = dfp_nz.index
dfp_nz_med = dfp_med.loc[dfp_nzi]
dfp_nz_std = dfp_std.loc[dfp_nzi]
dfp_nz_filt = dfp_nz.apply(lambda x: np.where((abs(x-dfp_nz_med)>dfp_nz_std*2) & (x>1.5), np.nan, x))
dfp_nz_filt.reset_index(inplace=True)
dfp_filt_mlt = dfp_nz_filt.melt(id_vars='Datetime')

df.reset_index(inplace=True)
dfp_filt_mlt.set_index(['Datetime', 'SiteID'], inplace=True)
df.set_index(['Datetime', 'SiteID'], inplace=True)

dfp_filt_i = dfp_filt_mlt.index
df_unfilt = df.loc[dfp_filt_i,:]
changed_ind = df_unfilt[df_unfilt['Value'].isnull() != dfp_filt_mlt['value'].isnull()].index
changed_df = df.loc[changed_ind,:]
changed_value_ids = changed_df['ValueID'].tolist()
changed_values_str = ','.join([str(a) for a in changed_value_ids])
select_sql = 'SELECT * FROM datavalues WHERE ValueID IN ({});'.format(changed_values_str)
df_changing = get_db_table_as_df('datavalues', sql=select_sql) 
date = str(datetime.datetime.now()).replace('.', '_')
date = date.replace(':', '_')
rain_file_name = 'bad_rain_values_{}.csv'.format(date)
df_changing.to_csv(rain_file_name)
base_sql = 'Update datavalues set {to_change} WHERE ValueID IN ({value_ids});'
qc_sql = base_sql.format(to_change='QCID=1', value_ids=changed_values_str)
val_sql = base_sql.format(to_change='Value=NULL', value_ids=changed_values_str)
con = sqlite3.connect(hr_db_filename)
c = con.cursor()
c.execute(qc_sql)
c.execute(val_sql)
con.commit()
con.close()
