#!/usr/bin/python2.7

import EdgarParser
from utils import *

import time
import numpy as np
import pandas as pd
import os.path
import pickle

import urllib3
import shutil
import certifi
from lxml import etree, html

SEC_homepage = 'https://www.sec.gov'

class MasterIndex():

	### class variable - edgar download url through amazon s3
	root_url = 'https://s3.amazonaws.com/indexes.sec.gov/full-index/'

	### class variable - saving directory
	save_dir	= '../IndexFiles/'

	### master index file header
	header_finish_line1 = 'CIK|Company Name|Form Type|Date Filed|Filename'
	header_finish_line2 = '---------------------------------------------'

	def __init__(self,year,qtr):
		### check if input has multiple years
		if '-' in year:
			year_range = year.split('-')
			self.year = [str(y) for y in np.arange(int(year_range[0]), int(year_range[1])+1,1)]
		elif ':' in year:
			year_range = year.split(':')
			self.year = [str(y) for y in np.arange(int(year_range[0]), int(year_range[1])+1,1)]
		elif ',' in year:
			self.year = [str(y) for y in year.split(',')]
		elif ';' in year:
			self.year = [str(y) for y in year.split(';')]
		else:
			self.year = [str(year)]

		### check if input has multiple quarters
		if '-' in qtr:
			qtr_range = qtr.split('-')
			self.qtr = [str(q) for q in np.arange(int(qtr_range[0]), int(qtr_range[1])+1,1)]
		elif ':' in qtr:
			qtr_range = qtr.split(':')
			self.qtr = [str(q) for q in np.arange(int(qtr_range[0]), int(qtr_range[1])+1,1)]
		elif ',' in qtr:
			self.qtr = [str(q) for q in qtr.split(',')]
		elif ';' in qtr:
			self.qtr = [str(q) for q in qtr.split(';')]
		else:
			self.qtr = [str(int(qtr))]

		self.corp_index = self.corp_index_parser()

	### download the master index files, save to local directory
	def download_index_files(self, index_filename, index_url):
		pool = urllib3.PoolManager(cert_reqs='CERT_REQUIRED',ca_certs=certifi.where())
		try:
			r = pool.request('GET', index_url, preload_content=False)
			if r.status == 200:
				with open(index_filename, 'wb') as f:
					shutil.copyfileobj(r, f)
				print(index_filename+' download success', r.status)	
			else:
				print(index_filename+' download failed', r.status)
			r.close()
		except urllib3.exceptions.SSLError as e:
			print(index_filename+' download failed', e)


	# parse the master index files
	def corp_index_parser(self):
		corp_index_columns = ['cik','company_name','form_type','date_filed','file_name','year','quarter']
		corp_index = pd.DataFrame(columns = corp_index_columns)
		index_file_list = [self.save_dir+'master_'+Y+'_'+Q+'.txt' for Y in self.year for Q in self.qtr]
		index_url_list =  [self.root_url + Y + '/QTR' + Q + '/master.idx' for Y in self.year for Q in self.qtr]
		year_list = [Y for Y in self.year for Q in self.qtr]
		quarter_list = [Q for Y in self.year for Q in self.qtr]

		for i in range(len(index_file_list)):
			index_filename = index_file_list[i]
			index_url = index_url_list[i]
			year = year_list[i]
			quarter = quarter_list[i]
			if os.path.isfile(index_filename) == False:
				# if index file doesn't exist, download from the SEC database on Amazon.
				self.download_index_files(index_filename, index_url)
			
			try:
				index_qtr = []
				with open(index_filename, 'r', encoding="ISO-8859-1") as f:
					print('Read Index file ' + index_filename)
					# read file line-by-line, skip the header, split with delimiter '|'
					skip_header = False
					for line in f:
						if skip_header == True:
							entry = line.replace('\n','').split('|')
							index_qtr.append(entry + [year, quarter])
						elif self.header_finish_line2 in line:
							skip_header = True
				index_qtr  = pd.DataFrame(np.array(index_qtr), columns = corp_index_columns)
				corp_index = pd.concat([corp_index, index_qtr],  ignore_index=True)
			except:
				pass

		return corp_index.set_index(np.arange(0,len(corp_index)))


class CorpFiling():

	def __init__(self,corp_index):
		# input: corp_index - a pandas Dataframe, same format as the MasterIndex.corp_index
		corp_index.set_index(np.arange(0,len(corp_index)))
		colname_current_list = corp_index.columns.tolist()
		colname_add_list = ['accession', 'url_index', 'url_html', 'url_txt', 'interactiveDataBtn', 'interactiveDataDict']
		for colname in colname_add_list:		
			if colname not in colname_current_list:
				corp_index.loc[:, colname] = ''	
		self.corp_index = corp_index

	# parse corpFiling urls
	def url_parser(self):
		print('parsing filing urls ( url_index, url_txt, url_html & interactiveDataBtn)')
		for idx in self.corp_index.index.tolist():
			ci = self.corp_index.loc[idx]

			if ci['accession'] == '':
				ci['accession'] = ci['file_name'].split('/')[-1].split('.')[0]

			if ci['url_index'] == '':
				ci['url_index'] = SEC_homepage + '/Archives/edgar/data/' + ci['cik'] + '/' \
						+ ci['accession'].replace('-','') + '/' + ci['accession'] + '-index.htm'

			if ci['url_txt'] == '':
				ci['url_txt'] = SEC_homepage+'/Archives/' + ci['file_name']
			
			if ci['url_html'] == '':
				ci['url_html'], ci['interactiveDataBtn'] =  EdgarParser.index_page_parser(ci['url_index'])
			
			if ci['interactiveDataDict'] == '':
				ci['interactiveDataDict'] = EdgarParser.interactiveDataBtn_page_parser(ci['interactiveDataBtn'])

			self.corp_index.loc[idx,:] = ci

	def filing_download(self, verbose=False):
		###create a connection pool to SEC
		pool = urllib3.connectionpool.HTTPSConnectionPool( "www.sec.gov" , maxsize = 4,
				cert_reqs='CERT_REQUIRED',ca_certs=certifi.where(), )

		for idx in self.corp_index.index.tolist():
			company = self.corp_index.loc[idx]
			print(company.cik + '\t' + company.company_name + '\t' + company.date_filed)

			###download html/txt files
			if '.htm' in company.url_html:
				web = webDownloader(company.url_html)
			else:
				web = webDownloader(company.url_txt)
			web.download(pool, verbose)

			###if interactiveURLXBRL exists, download interactive files
			if len( company.interactiveDataDict ) > 0:
				interactiveDataDict = eval(company.interactiveDataDict)
				try:
					for cat1_key in interactiveDataDict.keys():
						for cat2_key in interactiveDataDict[cat1_key]:
							input_url = interactiveDataDict[cat1_key][cat2_key]
							web = webDownloader(input_url)
							web.download(pool, verbose)
				except:
					pass

class webDownloader():
	# for an input url from SEC Edgar, search in local directory, 
	# if it can't find the file, request data online and save to local directory
	
	def __init__(self, input_url, save_dir='../Archives/edgar/data'):
		self.input_url = input_url
		self.file_dir = save_dir + input_url.split('data')[-1]
		if save_dir[-1] == '/': 
			save_dir = save_dir[:-1]
		self.save_dir = save_dir
	
	def read(self):
		if os.path.isfile(self.file_dir) == True:
			with open(self.file_dir,"rb") as f:
				return f.read()

	def readlines(self):
		if os.path.isfile(self.file_dir) == True:
			with open(self.file_dir) as f:
				return f.readlines()

	# download the file if the local directory doesn't exist
	def download(self, pool, verbose=False):
		# create local dir if not exists
		if not os.path.exists(self.save_dir):
			os.makedirs(self.save_dir)

		if os.path.isfile(self.file_dir) == True:
			if verbose: print('File found on local dir, download skipped!')
		elif len(self.file_dir) >0:
			try:
				r = pool.request('GET', self.input_url, preload_content=False)
				if r.status == 200:
					with open(self.file_dir, 'wb') as f:
						shutil.copyfileobj(r, f)
					if verbose: print('Download File '+self.input_url)	
				else:
					if verbose: print('Code: '+ r.status + ' Failed to download '+self.input_url)
				r.close()
			except:
				pass
			#except urllib3.exceptions.SSLError as e:
			#	print('Error download file', e)


