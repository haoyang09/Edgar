#!/usr/bin/python2.7

import time
import numpy as np
import pandas as pd
import os.path
import pickle

import urllib3
import shutil
import certifi

from lxml import etree, html



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

		self.index_data = self.index_data_parser()

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
	def index_data_parser(self):
		index_data_columns = ['cik','company_name','form_type','date_filed','file_name','year','quarter']
		index_data = pd.DataFrame(columns = index_data_columns)
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
				index_qtr  = pd.DataFrame(np.array(index_qtr), columns = index_data_columns)
				index_data = pd.concat([index_data, index_qtr],  ignore_index=True)
			except:
				pass

		return index_data.set_index(np.arange(0,len(index_data)))




class CorpFiling():

	SEC_homepage = 'https://www.sec.gov'

	def __init__(self,company_index):
		self.cik 						= company_index.cik
		self.company_name 				= company_index.company_name
		self.date_filed 				= company_index.date_filed
		self.form_type					= company_index.form_type
		self.file_name 					= company_index.file_name
		self.year						= company_index.year
		self.quarter					= company_index.quarter
		self.accession 					= self.file_name.split('/')[-1].split('.')[0]
		self.url_txt = ''
		self.url_index = ''
		self.url_html = ''
		self.interactiveDataBtn = ''
		self.interactiveDataDict = ''		

	# parse orpFiling urls
	def url_parser(self):
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
				self.interactiveDataBtn = self.SEC_homepage + href[0]
			'''			
			else:
				hrefs = tree.xpath('//a/@href')
				if len(hrefs)>0:
					href = [h for h in hrefs if '/cgi-bin/viewer?action=view' in h]
					self.interactiveDataBtn = self.SEC_homepage + href[0]			
				else:
					self.interactiveDataBtn = self.SEC_homepage + '/cgi-bin/viewer?action=view&amp;cik=' \
						+ self.cik + '&amp;accession_number=' + self.accession + '&amp;xbrl_type=v'
			'''
		else:
			print('Failed to get .HTML CorpFiling URL', status_code)

		self.interactiveDataDict = interactive_url_parser(self.interactiveDataBtn)


def interactive_url_parser(interactiveDataBtn):
	page, status_code = urllib3_request(interactiveDataBtn)
	interactiveDataDict = ''
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
				interactiveDataDict = create_interactive_url_dict(menu_text_cat1,menu_text_all,xbrl_urls)
		except:
			pass
	return interactiveDataDict

def create_interactive_url_dict(keys_cat1,keys_all,vals):
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


class webDownloader():
	# for an input url from SEC Edgar, search in local directory, 
	# if it can't find the file, request data online and save to local directory
	
	def __init__(self, pool, input_url, save_dir='../Archives/edgar/data'):
		if save_dir[-1] == '/': 
			save_dir = save_dir[:-1]
		self.pool = pool
		self.input_url = input_url
		self.save_dir = save_dir
		self.file_dir = save_dir + input_url.split('data')[-1]
	
	def read(self):
		if os.path.isfile(self.file_dir) == True:
			with open(self.file_dir,"rb") as f:
				return f.read()

	def readlines(self):
		if os.path.isfile(self.file_dir) == True:
			with open(self.file_dir) as f:
				return f.readlines()

	# download the file if the local directory doesn't exist
	def download(self):
		# create local dir if not exists
		if not os.path.exists(self.save_dir):
			os.makedirs(self.save_dir)

		if os.path.isfile(self.file_dir) == True:
			#print('File found on local dir, download skipped!')
			pass
		elif len(self.file_dir) >0:
			try:
				r = self.pool.request('GET', self.input_url, preload_content=False)
				if r.status == 200:
					with open(self.file_dir, 'wb') as f:
						shutil.copyfileobj(r, f)
					print('Download File '+self.input_url)	
				else:
					print('Code: '+ r.status + ' Failed to download '+self.input_url)
				r.close()
			except:
				pass
			#except urllib3.exceptions.SSLError as e:
			#	print('Error download file', e)



def getCIK_from_Ticker():
	# a lookup between CIF and stock symbol ticker
	# input a stock symbol or company name and return its CIK number
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
		print('urllib3_request failed to open' + input_url)
	return page, status_code


def remove_tags(text):
	tree = html.fromstring(text)
	text = tree.xpath("//text()")
	text = ''.join(text)
	text = text.replace("\n","")
	text = text.replace("/s/","")
	text = text.replace(u'\xa0',u"")
	return text

def filingParser(company_list): #(year_range, qtr_range, form_type, filings_filename):

	for i in range(len(company_list)):
		company_index = company_list.iloc[i]
		# create an instance of class edgar_filing #
		cf = CorpFiling(company_index)
		# use accession number to check if record exists in filings10K #
		cf_index = filings10K[filings10K.file_name == cf.file_name].index.tolist()
		print(str(i) + ': ' + company_index['cik'] + '\t' + company_index['date_filed'])
		if len(cf_index) == 0:
			# if record not exist in db, parsing SEC and update database #
			print('url not found in database, parse SEC to get urls')
			cf.url_parser()
			company_index.accession 			= cf.accession
			company_index.url_index 			= cf.url_index
			company_index.url_html 				= cf.url_html
			company_index.url_txt 				= cf.url_txt
			company_index.interactiveDataBtn 	= cf.interactiveDataBtn
			company_index.interactiveDataDict 	= cf.interactiveDataDict
			filings10K = filings10K.append(company_index, ignore_index=True)
			print('database length updated: ' + str(len(filings10K)))
		elif (  len(filings10K.loc[cf_index[0],'interactiveDataBtn'])  > 3 )  and \
			 (  len(filings10K.loc[cf_index[0],'interactiveDataDict']) < 3 ) :
			# update Interactive URL #
			cf.url_parser()
			filings10K.loc[cf_index, 'interactiveDataBtn']  = cf.interactiveDataBtn
			filings10K.loc[cf_index, 'interactiveDataDict'] = cf.interactiveDataDict
			print('interactive URLs: ', filings10K.loc[cf_index[0],'interactiveDataDict'])
		else:
			print('download skipped')

		# save the filings10K update to file
		if (i%2000==0) or (i==(len(company_list)-1)):
			print('update changes and save to file (csv and pkl)')
			filings10K = filings10K.set_index(np.arange(0,len(filings10K)))
			filings10K.to_csv(filings_filename.replace('pkl','csv'), sep='\t')
			with open(filings_filename, "wb") as f:
				pickle.dump(filings10K, f)


def download_filings(company_list):
	###create a connection pool to SEC
	pool = urllib3.connectionpool.HTTPSConnectionPool( "www.sec.gov" , maxsize = 4,
			cert_reqs='CERT_REQUIRED',ca_certs=certifi.where(), )

	for i  in range(len(company_list)):
		company = company_list.iloc[i]
		print(company.cik + '\t' + company.company_name + '\t' + company.date_filed)

		###download html/txt files
		if '.htm' in company.url_html:
			web = webDownloader(pool, company.url_html)
		else:
			web = webDownloader(pool, company.url_txt)
		web.download()

		###if interactiveURLXBRL exists, download interactive files
		if len( company.interactiveDataDict ) > 0:
			interactiveDataDict = eval(company.interactiveDataDict)
			try:
				for cat1_key in interactiveDataDict.keys():
					for cat2_key in interactiveDataDict[cat1_key]:
						input_url = interactiveDataDict[cat1_key][cat2_key]
						web = webDownloader(pool, input_url)
						web.download()
			except:
				pass


'''
def main():
	db_company_list_fname =  '../db_company_filing.pkl' 
	with open(db_company_list_fname,'r') as f:
		db_company_list = pickle.load(f)

	test = db_company_list.iloc[-1]
	input_url = test.url_html
	web = webDownloader([],input_url)
	page = web.read()
	page_text = remove_tags(page)
	print(page_text[:1000])


if __name__ == "__main__":
	main()

'''
