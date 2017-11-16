

import pandas as pd
import urllib3
import shutil
import certifi
import os.path
import numpy as np



file_dir = "/home/haoyang/Dropbox/JeanHao/Edgar/restate3/"
#file_dir = "C:\\Users\\JeanZeng\\Dropbox\\JeanHao\\Edgar\\restate3\\"


pool = urllib3.connectionpool.HTTPSConnectionPool( "www.sec.gov" , maxsize = 1,
		cert_reqs='CERT_REQUIRED',ca_certs=certifi.where(), )

resfile = file_dir + "restate3.csv"

db = pd.read_csv(resfile)

print(db)

search_key = [ 'deliverable arrangement',
		'deliverable revenue arrangement',
		'element arrangement',
		'elements arrangement',
		'vsoe',
		'TPE',
		]

SEC_url = "www.sec.gov"

for ii,key in enumerate(search_key):
	db[key] = 0

for ii in range(len(db)):

	input_url =  db.iloc[ii].HTTP_NAME_TEXT
	key_freq = np.zeros(len(search_key))

	if len(str(input_url)) > 5:
		save_dir = file_dir + input_url.split('/')[-1]
		if os.path.isfile(save_dir) == False:
			r = pool.request('GET',  input_url, preload_content=False)
			if r.status == 200:
				with open( save_dir, 'w') as f:
					shutil.copyfileobj(r, f)
		lines = []
		try:
			with open( save_dir, 'r') as f:
				lines = f.readlines()
		except:
			pass

		for line in lines:
			for kk, key in enumerate(search_key):
				if key in line.lower():
					key_freq[kk] = key_freq[kk] + 1
	
	print(key_freq)	
	for [key,freq] in zip(search_key,key_freq):	
		db.loc[ii,key] = freq

db.to_csv(file_dir + 'restate3_search_result.csv', sep='\t',encoding = 'utf-8')

