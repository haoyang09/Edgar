
from edgar import *
import pandas as pd
import urllib2
import urllib3
import shutil
import certifi
import os.path
import numpy as np
import PyPDF2


file_dir = "/home/haoyang/Dropbox/JeanHao/Edgar/"


pool = urllib3.connectionpool.HTTPSConnectionPool( "www.sec.gov" , maxsize = 1,
		cert_reqs='CERT_REQUIRED',ca_certs=certifi.where(), )

resfile = file_dir + "commentletter20170404_sep.csv"

db = pd.read_csv(resfile,sep='\t')

db['period_of_report'] = ''



def grab_period(input_url):
	period = ''
	try:
		page, status_code = urllib3_request('https://www.sec.gov' + input_url)
		if status_code == 200:
			tree = html.fromstring(page)
			period = tree.xpath('//div[text() = "Period of Report"]/following-sibling::div/text()')[0]
	except:
		pass
	return period


for ii in range(len(db)):
	input_url =db.loc[ii,'LIST_HTTP_NMEHTML_ED']  
	period  = grab_period(input_url)
	db.loc[ii,'period_of_report'] = period
	print(ii, period)

db.to_csv(file_dir + 'commentletter20170404_sep_grabperiod.csv', sep='\t',encoding = 'utf-8')
