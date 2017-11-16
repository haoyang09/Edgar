import urllib3
import shutil
import certifi
import numpy as np
import pandas as pd
import os.path
import pickle
import time
from lxml import etree, html

import edgar 



menu_key2 = 'accounting polic'
menu_key1 = 'notes to'
search_key = [ 'deliverable arrangement',
			'deliverable revenue arrangement',
			'element arrangement',
			'elements arrangement',
			'vsoe',
			]
print('search keywords:', ','.join(s for s in search_key) )

''' load company_filing DataBase '''
company_list_dir = '/'.join(edgar.master_index.index_save_dir.split('/')[0:-2]) + '/'
company_list_fname = company_list_dir + 'db_company_filing.pkl'
if os.path.isfile(company_list_fname) == True:
	print('loading master_index from database')
	with open(company_list_fname, "rb") as f:
		company_list_db = pickle.load(f)
else:
	company_list_db = pd.DataFrame(columns = company_list.columns)

company_list = company_list_db[ company_list_db['accession'] == '0001193125-09-214859' ] 
company_list = company_list.iloc[0]
test = edgar.company_filing(company_list)
test.FilingURLPraser()
test.IntActURLParser()
print(test.url_IntActDict)

