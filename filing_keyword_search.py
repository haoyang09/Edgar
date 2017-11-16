import os.path
import urllib3
import shutil
import certifi
import numpy as np
import pandas as pd

#from bs4 import BeautifulSoup
#import re
from lxml import etree, html

import edgar

from joblib import Parallel, delayed
import multiprocessing


def main():

	###################### User Input ###########################
	
	### code_testing - run a small amount of companies
	code_testing = True	

	### output result into a CSV file.
	output_file_name = 'keyword_search_test_20170117.csv'
	
	### directory for quarterly master index files provided by SEC
	### on Ubuntu
	edgar.master_index.index_save_dir	= '/home/haoyang/Dropbox/JeanHao/Edgar/IndexFiles/'
	### on Mac
	#edgar.master_index.index_save_dir	= '/Users/haoyang/Dropbox/JeanHao/Edgar/IndexFiles/'
	#edgar.master_index.index_save_dir	= 'C:\\Users\\JeanZeng\\Dropbox\\JeanHao\\Edgar\\IndexFiles\\'
	#edgar.master_index.index_save_dir	= 'C:\\Users\\Owner\\Dropbox\\JeanHao\\Edgar\\IndexFiles\\'

	### year and quarter range
	year_range = '2016'
	qtr_range  = '1'

	### form types to perform search in
	form_type = ['10-K','10-K/A']
	
	### search for keywords in the Note section
	search_key = [ 'deliverable arrangement',
					'element arrangement',
					'elements arrangement',
					'vsoe',
			]

	###################### main scripts ###########################
	
	### load master index database
	master_index = edgar.master_index( year_range, qtr_range)
	### get company_list for the specified year, quarter and form_types
	company_list = master_index.index_data.loc[master_index.index_data['form_type'].isin(form_type) ]
	company_list = company_list.set_index([range(0,len(company_list))]) ### reset index values 
	print(str(len(company_list)) + ' filings found for form type' + ''.join(s for s in form_type))
	
	### initialize the search result values	
	for col in search_key:
		if col not in company_list.columns:
			company_list.loc[:,col] = ''

	### run parallel pricessing 
	num_cores = multiprocessing.cpu_count()
	print('num of cores: '+ str(num_cores))
	if code_testing == True:
		print('Code testing - only a small list is processed ')
		search_result = Parallel(n_jobs=num_cores)(delayed(parallel_search_task)(company_list.iloc[i],search_key, i) for i in range(0, 4*num_cores))
	else:
		search_result = Parallel(n_jobs=num_cores)(delayed(parallel_search_task)(company_list.iloc[i],search_key, i) for i in range(0, len(company_list)))
	
	### search_result returns a list, each element represents one row in the company_list pd DataFrame 	
	for i, item in enumerate(search_result):
		company_list.loc[i] = item
	### output result to CSV file	
	company_list.to_csv( output_file_name , sep='\t', encoding='utf-8')
'''
		#if os.path.isfile(self.master_index_DB_filename) == True:
		#	print('loading master_index from database')
		#	with open(self.master_index_DB_filename, "rb") as f:
		#		self.index_data = pickle.load(f)
	#with open(self.master_index_DB_filename, "wb") as f:
		#	pickle.dump(self.index_data, f)	
'''

def parallel_search_task(company_index, search_key, i):
	print(str(i) + ': ' + company_index['cik'] + '\t' + company_index['company_name'])

	### create an instance of class edgar_filing 
	f1 = edgar.company_filing(company_index)
	
	### if interactiveURLXBRL exists
	if len(f1.filing_url_interactiveXBRL) > 0: 
		search_result = ['' for s in search_key]
		### search keywords in each part of the filing
		for cat1_key in f1.filing_url_interactiveXBRL.keys():
			for cat2_key in f1.filing_url_interactiveXBRL[cat1_key]:
				#if "note" in cat1_key.lower() or "note" in cat2_key.lower(): 
				input_url = f1.SEC_homepage + f1.filing_url_interactiveXBRL[cat1_key][cat2_key]
				print(input_url)
				r1 = single_search_tast( input_url , search_key	 )
				for j, item in enumerate(r1):
					if item == True:
						search_result[j] = ''.join([search_result[j], cat1_key+'-'+cat2_key, '; '])
	### if interactiveURLXBRL doesn't exists	
	else:  
		### search the single html filing file
		search_result = single_search_tast( f1.filing_url_html, search_key )
	
	### output result
	for i, key in enumerate(search_key):
		company_index.loc[key] = search_result[i]
	return company_index

def single_search_tast(input_url, search_key):
	page, status_code = edgar.urllib3_request( input_url )
	if status_code == 200:
		tree = html.fromstring(page)
		div_text = tree.xpath('//div/text()')
		search_result = [any(x in t.lower() for t in div_text) for x in search_key]
		return search_result
	else:
		return [False for s in search_key]
'''
	### get filing document from SEC website (HTM format)
	filing_data_html, status_code = edgar.urllib3_request(f1.filing_url_html)

	if status_code == 200:
		input_text = filing_data_html
		soup = BeautifulSoup(input_text.decode('utf-8', 'ignore'),"lxml")
		all_div_tags = [s.text for s in soup.find_all('div') ]  ## tag "a", "td"

		### for each keyword in the search_key list, check if it's in the filing
		#search_result = [any( x in tag.lower() for tag in all_div_tags) for x in search_key]
		for key in search_key:
			if any( key in tag.lower() for tag in all_div_tags):
				company_index.loc[key] = True
				print(str(i) + ': ' + company_index['cik'] + '\t' + company_index['company_name'] +  '\t\t' + key)
	return company_index
	
'''	


def KeywordSearch_in_Filing_Notes(input_text, search_key):
	### make soup
	#soup = BeautifulSoup(input_text.decode('utf-8', 'ignore'),"lxml")
	soup = BeautifulSoup(input_text,"lxml")

	### parse the text
	for link in soup.find_all('a'):
		link_url = link.get('href')
		if '/Archives/edgar/data/' in link_url:
			return 'https://www.sec.gov' + link_url


if __name__ == '__main__':
	main()
