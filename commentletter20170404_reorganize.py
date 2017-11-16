
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

resfile = file_dir + "commentletter20170404.csv"

db = pd.read_csv(resfile)

db_output = pd.DataFrame(columns = db.columns.values)


count = 0
for ii in range(len(db)):

	print(ii)
	line = db.iloc[ii]
	#print(ii,line)

	try:
		LIST_FORM_FKEY_ED = db.loc[ii,'LIST_FORM_FKEY_ED'].split('|')
		#print(ii, LIST_FORM_FKEY_ED)

		new_entry = line
		if len(LIST_FORM_FKEY_ED) == 1:
			db_output = db_output.append(new_entry, ignore_index=True)
			count = count + 1
		else:
			LIST_FILE_DATE_ED = db.loc[ii,'LIST_FILE_DATE_ED'].split('|')
			LIST_FILE_DATE_ED_len = len(LIST_FILE_DATE_ED)
			LIST_HTTP_NMEHTML_ED = db.loc[ii,'LIST_HTTP_NMEHTML_ED'].split('|')
			LIST_HTTP_NMEHTML_ED_len = len(LIST_HTTP_NMEHTML_ED)

			for jj in range(len(LIST_FORM_FKEY_ED)):
				new_entry = line
				new_entry.loc['LIST_FORM_FKEY_ED'] 		= LIST_FORM_FKEY_ED[jj]
				if jj> LIST_FILE_DATE_ED_len-1:
					new_entry.loc['LIST_FILE_DATE_ED'] = ' '
				else:
					new_entry.loc['LIST_FILE_DATE_ED'] 		= LIST_FILE_DATE_ED[jj]
		
				if jj> LIST_HTTP_NMEHTML_ED_len-1:
					new_entry.loc['LIST_HTTP_NMEHTML_ED'] = ' '
				else:
					new_entry.loc['LIST_HTTP_NMEHTML_ED'] 	= LIST_HTTP_NMEHTML_ED[jj]
				db_output = db_output.append(new_entry, ignore_index=True)
				count = count + 1
				#print(count, LIST_FORM_FKEY_ED[jj])
	except:
		pass

db_output.to_csv(file_dir + 'commentletter20170404_sep.csv', sep='\t',encoding = 'utf-8')
