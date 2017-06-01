from main_db_script import parse_wml2_data
site_number = "364715076030801"
start_date = "2005-03-11"
url = "http://waterservices.usgs.gov/nwis/dv/?format=waterml,2.0&sites={}&startDT={}" .format(site_number, start_date)
gw_data = parse_wml2_data(url, "USGS")
