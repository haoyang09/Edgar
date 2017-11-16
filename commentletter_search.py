
from edgar import *

import pandas as pd
import urllib2
import urllib3
import shutil
import certifi
import os.path
import numpy as np
import PyPDF2


def download_PDF_file(download_url,save_file):
    response = urllib2.urlopen(download_url)
    file = open(save_file, 'w')
    file.write(response.read())
    file.close()
    print("Download PDF file completed")

file_dir = "/home/haoyang/Dropbox/JeanHao/Edgar/commentletter/"


pool = urllib3.connectionpool.HTTPSConnectionPool( "www.sec.gov" , maxsize = 1,
		cert_reqs='CERT_REQUIRED',ca_certs=certifi.where(), )

resfile = file_dir + "commentletter.csv"

db = pd.read_csv(resfile)

print(db['ISS_ACCRL_DISC_TEXT'])

search_R_column_key = ["revenue rec",
					]

search_key = [ 'deliverable arrangement',
		'deliverable revenue arrangement',
		'element arrangement',
		'elements arrangement',
		'vsoe',
		'TPE',
		'residual method',
		]

SEC_url = "www.sec.gov"

for ii,key in enumerate(search_R_column_key):
	db[key] = 0
for ii,key in enumerate(search_key):
	db[key] = 0

count = 0

for ii in range(len(db)):

	key_exist = np.zeros(len(search_R_column_key))
	key_freq = np.zeros(len(search_key))

	line  = db.loc[ii,'ISS_ACCRL_DISC_TEXT']
	if len(str(line))>5:
		for kk,key in enumerate(search_R_column_key):
			if key in line.lower():
				key_exist[kk] = 1
				count = count + 1

	### input_url of the comment letter (use pdf if it exists)
	input_url =  db.loc[ii,'HTTP_NAME_TEXT']
	try:
		page, status_code = urllib3_request('https://www.sec.gov' + db.loc[ii,'HTTP_NAME_HTML'])
		if status_code == 200:
			tree = html.fromstring(page)
			hrefs = tree.xpath('//td/a/@href')
			#print(hrefs)
			input_url = hrefs[0]
			for href in hrefs:
				if '.pdf' in href:
					input_url = href
					break
				elif '.htm' in href:
					input_url = href
					break
	except:
		pass

	if len(str(input_url)) > 5 and key_exist>0:
		print(ii, input_url)
		### save comment letter file
		if '.pdf' in input_url:
			comment_letter_file_name = file_dir + input_url.split('/')[-2] + '-' + input_url.split('/')[-1]
			if_pdf_file = True
		elif '.htm' in input_url:
			comment_letter_file_name = file_dir + input_url.split('/')[-2] + '-' + input_url.split('/')[-1]
			if_pdf_file = False
		else:
			comment_letter_file_name = file_dir + input_url.split('/')[-1]
			if_pdf_file = False
		#check if local file exists
		if os.path.isfile(comment_letter_file_name) == False:
			if if_pdf_file == False:
				r = pool.request('GET',  input_url, preload_content=False)
				if r.status == 200:
					with open( comment_letter_file_name, 'w') as f:
						try:
							shutil.copyfileobj(r, f)
							print('Download TXT file completed')
						except:
							print('unable to download file')
			else:
				download_PDF_file('https://www.sec.gov'+input_url,comment_letter_file_name)

		### load comment letter file
		comment_letter = ''
		try:
			with open( comment_letter_file_name ) as f:
				if if_pdf_file==True:
					read_pdf = PyPDF2.PdfFileReader(f)
					number_of_pages = read_pdf.getNumPages()
					for pgnum in range(number_of_pages):
						page = read_pdf.getPage(pgnum)
						page_content = page.extractText()
						comment_letter = comment_letter + page_content
					#print(comment_letter)
				else:
					comment_letter = f.read()
					#print(comment_letter)
		except:
				print('Error opening file')
		

		### check if search key in the letter
		for kk, key in enumerate(search_key):
			if key in comment_letter.lower():
				key_freq[kk] = key_freq[kk] + 1
		print(key_freq)

	### add search result to the spreadsheet
	for [key,flag] in zip(search_R_column_key,key_exist):	
		db.loc[ii,key] = flag
	for [key,flag] in zip(search_key,key_freq):	
		db.loc[ii,key] = flag

print(count)
db.to_csv(file_dir + 'commentletter_search_result.csv', sep='\t',encoding = 'utf-8')

