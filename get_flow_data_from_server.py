from main_db_script import parse_wml2_data
from datetime import datetime, timedelta
import pandas as pd
site_number = "0204288771"
parameter_id = '00060'

# service only allows getting data for 120 days at a time.
start_date = '2015-07-17'
end_date = pd.datetime.today().strftime('%Y-%m-%d')
dates = pd.date_range(start_date, end_date, freq='120D')
date_list = dates.strftime('%Y-%m-%d').tolist()
date_list.append(end_date)

for i in range(len(date_list)-1):
    url = "http://waterservices.usgs.gov/nwis/iv/" \
          "?format=waterml,2.0&sites={}&startDT={}&endDT={}&parameterCd={}"
    url = url.format(site_number, date_list[i], date_list[i+1], parameter_id)
    print 'getting flow data for {}'.format(date_list[i])
    flow_data = parse_wml2_data(url, "USGS")

