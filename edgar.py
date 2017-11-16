#!/usr/bin/python2.7

import urllib3
import shutil
import certifi
import numpy as np
import pandas as pd
import os.path
import pickle
from lxml import etree, html
import time


def main():
	db_company_list_fname =  master_index.db_dir + 'db_company_filing.pkl' 
	with open(db_company_list_fname,'r') as f:
		db_company_list = pickle.load(f)

	test = db_company_list.iloc[-1]
	input_url = test.url_html
	web = webDownloader([],input_url)
	page = web.read()
	page_text = remove_tags(page)
	print(page_text[:1000])


def remove_tags(text):
	tree = html.fromstring(text)
	text = tree.xpath("//text()")
	text = ''.join(text)
	text = text.replace("\n","")
	text = text.replace("/s/","")
	text = text.replace(u'\xa0',u"")
	return text

############################
##  self-defined classes  ##
############################


class master_index():

	### class variable - edgar download url through amazon s3
	index_root_url = 'https://s3.amazonaws.com/indexes.sec.gov/full-index/'

	### class variable - saving directory
	index_save_dir	= '/home/haoyang/Edgar/IndexFiles/'

	### company_filing database
	db_dir = '/'.join(index_save_dir.split('/')[:-2]) + '/'

	''' edgar filing files save directory '''
	filing_save_dir = '/home/haoyang/Edgar/'

	### master index column 
	index_columns = ['cik','company_name','form_type','date_filed','file_name']

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

		### load master_index from database file
		#self.master_index_DB_filename = self.index_save_dir+'master_index.pkl'
		#if os.path.isfile(self.master_index_DB_filename) == True:
		#	print('loading master_index from database')
		#	with open(self.master_index_DB_filename, "rb") as f:
		#		self.index_data = pickle.load(f)
		#else:
		#	### if database not exists, create empty Data Frame
		#	self.index_data = pd.DataFrame(columns = self.index_columns)
		self.index_data = pd.DataFrame(columns = self.index_columns)

		self.downloadIndex()
		self.getIndex()

	### Download index file and save to local directory
	def downloadIndex(self):
		for year in self.year:
			for qtr in self.qtr:
				master_index_filename = self.index_save_dir + 'master_' + year + '_' + qtr + '.txt'
				if os.path.isfile(master_index_filename) == False:
					index_url = self.index_root_url + year + '/QTR' + qtr + '/master.idx'
					pool = urllib3.PoolManager(cert_reqs='CERT_REQUIRED',ca_certs=certifi.where())
					try:
						r = pool.request('GET', index_url, preload_content=False)
						if r.status == 200:
							with open(master_index_filename, 'wb') as f:
								shutil.copyfileobj(r, f)
							print('Year:'+ year +' QTR:' + qtr + ' Download SEC Index file Success!', r.status)	
						else:
							print('Year:'+ year +' QTR:' + qtr + ' Download SEC Index file Failed!', r.status)
						r.close()
					except urllib3.exceptions.SSLError as e:
						print('Year:'+ year +' QTR:' + qtr + ' Download SEC Index file Failed!', e)


	### Get the master index from downloaded index files
	def getIndex(self):

		for year in self.year:
			for qtr in self.qtr:
				master_index_filename = self.index_save_dir + 'master_' + year + '_' + qtr + '.txt'

				### if the master_indx of given year/qtr doesn't exist in the database, load from master file
				if os.path.isfile(master_index_filename) == True:
					with open(master_index_filename, 'r') as f:
						print('Read Index file: Year:'+year+' QTR:'+qtr)
						### read file line-by-line, skip the header, split with delimiter '|'
						skip_header = False
						index_qtr = []
						for line in f:
							if skip_header == True:
								index_qtr.append(line.replace('\n','').split('|'))
							elif self.header_finish_line2 in line:
								skip_header = True
						### convert the index_qtr to a DataFrame
						index_qtr = np.array(index_qtr)
						try:
							index_qtr_df = pd.DataFrame( index_qtr,
										columns = self.index_columns,
										index=list(range(0,index_qtr.shape[0],1)))
							self.index_data = pd.concat([self.index_data, index_qtr_df], ignore_index=True)
						except:	
							print('Failed to convert quarterly index to pd DataFrame')
		
		''' reset the index row number '''
		self.index_data = self.index_data.set_index([range(0,len(self.index_data))])
		''' save the master_index into database file '''
		#with open(self.master_index_DB_filename, "wb") as f:
		#	pickle.dump(self.index_data, f)


class company_filing():

	SEC_edgar = 'https://www.sec.gov/Archives/edgar/'
	SEC_homepage = 'https://www.sec.gov'

	def __init__(self,company_index):
		self.cik 						= company_index.cik
		self.company_name 				= company_index.company_name
		self.date_filed 				= company_index.date_filed
		self.form_type					= company_index.form_type
		self.file_name 					= company_index.file_name

		self.accession 					= self.file_name.split('/')[-1].split('.')[0]
		### filing urls
		self.url_index 			= ''
		self.url_txt 			= ''
		self.url_html 			= ''
		self.url_IntActBtn 		= ''
		self.url_IntActDict 		= ''

	def FilingURLPraser(self):
		self.url_txt 	= self.SEC_homepage + '/Archives/' + self.file_name	
		self.url_index 	= self.SEC_homepage + '/Archives/edgar/data/' + self.cik + '/' \
					+ self.accession.replace('-','') + '/' + self.accession + '-index.htm'

		page, status_code = urllib3_request(self.url_index)
		if status_code == 200:
			### get the url of the html
			tree = html.fromstring(page)
			#href = tree.xpath('//td[text() = "10-K"]/preceding-sibling::td/a/@href') ### hard coded form_type "10-K"
			href = tree.xpath('//td/a/@href')
			self.url_html = self.SEC_homepage + href[0]
			
			### get the url of the interactive data button
			href = tree.xpath('//a[@id="interactiveDataBtn"]/@href')
			if len(href)>0:
				self.url_IntActBtn = self.SEC_homepage + href[0]
#			elif len(href)==0:
#				hrefs = tree.xpath('//a/@href')
#				href = [h for h in hrefs if '/cgi-bin/viewer?action=view' in h]
#				self.url_IntActBtn = self.SEC_homepage + href[0]			
#			else:
#				self.filing_url_interactive = self.SEC_homepage + '/cgi-bin/viewer?action=view&amp;cik=' \
#					+ self.cik + '&amp;accession_number=' + self.accession + '&amp;xbrl_type=v'
		else:
			print('Failed to get .HTML Filing URL', status_code)

	def IntActURLParser(self):
		page, status_code = urllib3_request(self.url_IntActBtn)
		if status_code == 200:
			tree = html.fromstring(page)
			try:
				#javascript_text = tree.xpath('//script[@type="text/javascript" and @language="javascript"]/text()')
				javascript_text = tree.xpath('//script[@type="text/javascript"]/text()')
				xbrl_urls = [line.split('=')[-1].replace('"','').replace(' ','') for line in javascript_text[0].split(';') 
								if '/Archives/edgar/data/' in line and (('.htm' in line) or ('.xml' in line))]
				menu_text_cat1 = tree.xpath('//ul[@id = "menu"]/child::li[@class="accordion"]/a/text()')
				menu_text_all = tree.xpath('//ul[@id = "menu"]/descendant::li[@class="accordion"]/a/text()')
				if len(xbrl_urls) > 0:
					self.url_IntActDict = CreateInteractiveURLDict(menu_text_cat1,menu_text_all,xbrl_urls)
			except:
				pass


class webDownloader():
	### for an input url from SEC Edgar, search in local directory, 
	### if it can't find the file, request data online and save to local directory

	#root_dir =  '/'.join(master_index.index_save_dir.split('/')[0:-2]) + '/'
	root_dir = master_index.filing_save_dir
	
	def __init__(self, pool, input_url):
		self.pool = pool
		self.input_url = input_url
		self.local_file_dir = self.root_dir + input_url
		self.local_folder_dir = os.path.dirname(self.local_file_dir)
		### create local dir if not exists
		if not os.path.exists(self.local_folder_dir):
			os.makedirs(self.local_folder_dir)
		self.download()
	
	def read(self):
		if os.path.isfile(self.local_file_dir) == True:
			with open(self.local_file_dir,"r") as f:
				return f.read()
	def readlines(self):
		if os.path.isfile(self.local_file_dir) == True:
			with open(self.local_file_dir) as f:
				return f.readlines()

	### download the file if the local directory doesn't exist
	def download(self):
		if os.path.isfile(self.local_file_dir) == True:
			#print('File found on local dir, download skipped!')
			pass
		elif len(self.local_file_dir) >0:
			try:
				r = self.pool.request('GET', self.input_url, preload_content=False)
				if r.status == 200:
					with open(self.local_file_dir, 'wb') as f:
						shutil.copyfileobj(r, f)
					print('Download File '+self.input_url)	
				else:
					print('Code: '+ r.status + ' Failed to download '+self.input_url)
				r.close()
			except:
				pass
			#except urllib3.exceptions.SSLError as e:
			#	print('Error download file', e)


def CreateInteractiveURLDict(keys_cat1,keys_all,vals):
	interactiveURL_dict = {}
	### create level-1 dict
	j = 0
	k = 0
	for i in range(len(keys_all)):
		if keys_all[i] in keys_cat1:
			j = i
			interactiveURL_dict[keys_all[j]] = {}
		else:
			interactiveURL_dict[keys_all[j]][keys_all[i]] = vals[k]
			k = k + 1
	return str(interactiveURL_dict)

	
def URLDataBase():
	pass


def getCIK_from_Ticker():
	### a lookup between CIF and stock symbol ticker
	### input a stock symbol or company name and return its CIK number
	pass


def urllib3_request(input_url):
	page = []
	status_code = []
	pool = urllib3.PoolManager(cert_reqs='CERT_REQUIRED',ca_certs=certifi.where())
	try:
		r = pool.request('GET', input_url)
		status_code = r.status
		page = r.data
		r.close()
	except:
		print('urllib3_request can not open url')
	return page, status_code




if __name__ == "__main__":
	main()
