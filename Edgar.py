from utils import *

import time
import numpy as np
import pandas as pd
import os.path
import pickle
from tqdm import tqdm
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

		self.corpidx = self.corpidx_parser()

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
	def corpidx_parser(self):
		corpidx_columns = ['cik','company_name','form_type','date_filed','file_name','year','quarter']
		corpidx = pd.DataFrame(columns = corpidx_columns)
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
				index_qtr  = pd.DataFrame(np.array(index_qtr), columns = corpidx_columns)
				corpidx = pd.concat([corpidx, index_qtr],  ignore_index=True)
			except:
				pass

		return corpidx.set_index(np.arange(0,len(corpidx)))


class CorpFiling():

	def __init__(self,corpidx):
		# input: corpidx - a pandas Dataframe, same format as the MasterIndex.corpidx
		corpidx.set_index(np.arange(0,len(corpidx)))
		colname_current_list = corpidx.columns.tolist()
		colname_add_list = ['accession', 'url_index', 'url_html', 'url_txt', 'interactiveDataBtn',\
							 'interactiveDataDict', 'url_13Ftable_html', 'period_of_report']
		for colname in colname_add_list:		
			if colname not in colname_current_list:
				corpidx.loc[:, colname] = ''	
		self.corpidx = corpidx

	# parse corpFiling urls
	def url_parser(self):
		print("parse filing urls")
		for idx in tqdm(self.corpidx.index.tolist()):
			ci = self.corpidx.loc[idx]

			if ci['accession'] == '':
				ci['accession'] = ci['file_name'].split('/')[-1].split('.')[0]

			if ci['url_index'] == '':
				ci['url_index'] = SEC_homepage + '/Archives/edgar/data/' + ci['cik'] + '/' \
						+ ci['accession'].replace('-','') + '/' + ci['accession'] + '-index.htm'

			if ci['url_txt'] == '':
				ci['url_txt'] = SEC_homepage+'/Archives/' + ci['file_name']
			
			if ci['period_of_report'] == '':
				ci['period_of_report'] = period_of_report_parser(ci['url_index'])

			if '10-K' in ci['form_type']:
				if ci['url_html'] == '':
					ci['url_html'], ci['interactiveDataBtn'] =  index_page_parser(ci['url_index'])
			
				if ci['interactiveDataDict'] == '':
					ci['interactiveDataDict'] = interactiveDataBtn_page_parser(ci['interactiveDataBtn'])

			elif ( "13F" in ci['form_type'] ) and ci['url_13Ftable_html'] == '':
				ci['url_html'], ci['url_13Ftable_html'] = index_page_parser_13F(ci['url_index'])

			self.corpidx.loc[idx,:] = ci

	def period_of_report_parser(self):
		print("parse period of report")
		for idx in tqdm(self.corpidx.index.tolist()):
			ci = self.corpidx.loc[idx]
			if ci['period_of_report'] == '':
				ci['period_of_report'] = period_of_report_parser(ci['url_index'])
				
			

	def filing_download(self, verbose=False):
		###create a connection pool to SEC
		pool = urllib3.connectionpool.HTTPSConnectionPool( "www.sec.gov" , maxsize = 4,
				cert_reqs='CERT_REQUIRED',ca_certs=certifi.where(), )

		for idx in tqdm(self.corpidx.index.tolist()):
			company = self.corpidx.loc[idx]
			if verbose: 
				print("download filing files for "+ company.cik + '\t' + company.company_name + '\t' + company.date_filed)

			if "10-K" in company.form_type:
				###download html/txt files
				if '.htm' in company.url_html:
					web = webDownloader(company.url_html)
				else:
					web = webDownloader(company.url_txt)
				web.download(pool, verbose = verbose)

				###if interactiveURLXBRL exists, download interactive files
				if len( company.interactiveDataDict ) > 0:
					interactiveDataDict = eval(company.interactiveDataDict)
					try:
						for cat1_key in interactiveDataDict.keys():
							for cat2_key in interactiveDataDict[cat1_key]:
								input_url = interactiveDataDict[cat1_key][cat2_key]
								web = webDownloader(input_url)
								web.download(pool, verbose = verbose)
					except:
						pass

			elif "13F" in company.form_type:
				'''
				### download primary html, infotable html or txt file
				try: 
					web = webDownloader(company.url_html)
					web.download(pool, verbose = verbose)
				except: 
					print("failed to download url_html: " + company.url_html)
				'''

				try:
					if len(company.url_13Ftable_html) > 0:
						web = webDownloader(company.url_13Ftable_html)
					else:
						web = webDownloader(company.url_txt)
					web.download(pool, verbose = verbose)
				except: 
					print("failed to download url_13Ftable_html: " + company.url_13Ftable_html)

	def table_13F_parser(self, verbose=False):
		self.tables_13F = pd.DataFrame([])
		for idx in tqdm(self.corpidx.index.tolist()):
			company = self.corpidx.loc[idx]
			try:
				if ("13F" in company.form_type) and ("13F-NT" not in company.form_type):
					#print(company.cik + '\t' + company.company_name + '\t' + company.date_filed)
					web = webDownloader(company.url_13Ftable_html)
					table = web.read_table_13F()
					table["cik"] 			= company.cik
					table["company_name"] 	= company.company_name
					table["form_type"] 		= company.form_type
					table["date_filed"] 	= company.date_filed
					table["url_index"] 		= company.url_index
												
					if len(table)>0:
						if len(self.tables_13F) == 0:
							self.tables_13F = table
						else:
							self.tables_13F = pd.concat([self.tables_13F, table], ignore_index = True)
			except Exception as e:
				print(e, company.cik + '\t' + company.company_name + '\t' + company.date_filed)




class webDownloader():
	# for an input url from SEC Edgar, search in local directory, 
	# if it can't find the file, request data online and save to local directory
	
	def __init__(self, input_url, save_dir='../Archives/edgar/data'):
		self.input_url = input_url
		self.file_dir = save_dir + input_url.split('data')[-1]
		self.save_dir = self.file_dir.replace(self.file_dir.split('/')[-1],"")
	
	def read(self):
		if os.path.isfile(self.file_dir) == True:
			with open(self.file_dir,"rb") as f:
				return f.read()

	def readlines(self):
		if os.path.isfile(self.file_dir) == True:
			with open(self.file_dir) as f:
				return f.readlines()

	def read_table_13F(self):
		if os.path.isfile(self.file_dir) == True:
			with open(self.file_dir) as f:
				return table_13F_parser(f)

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
					if verbose: print('[Success] Download file from'+self.input_url)	
				else:
					if verbose: print('[Failed] Download file from'+self.input_url + '--Status Code:'+ r.status)
				r.close()
			except:
				pass
			#except urllib3.exceptions.SSLError as e:
			#	print('Error download file', e)


###########################################################################


def index_page_parser_13F(url_index):
	'''
	<td scope="row"><a href="/Archives/edgar/data/1000097/000091957416011102/xslForm13F_X01/infotable.xml">infotable.html</a></td>
	'''
	page, status_code = urllib3_request(url_index)
	url_html = ''
	url_13Ftable_html = ''
	if status_code == 200:
		tree = html.fromstring(page)
		href_list = tree.xpath('//td/a/@href')
		text_list  = tree.xpath('//td/a/text()')

		if len(href_list)>0: 
			url_html = 'https://www.sec.gov' + href_list[0]
		if len(href_list)>2: 
			url_13Ftable_html = 'https://www.sec.gov' + href_list[2]

		try:
			for text, href in zip(text_list, href_list):
				if "primary" in text and "html" in text:
					url_html = 'https://www.sec.gov' + href
				elif "table" in text and "html" in text:
					url_13Ftable_html =  'https://www.sec.gov' + href
		except: pass
	else:
		print('Failed to open url ' + url_index + status_code)
	return 	url_html, url_13Ftable_html

def table_13F_parser(url_infotable):
	try:
		table = pd.read_html(url_infotable, header = 2)
		return pd.DataFrame(table[3])
	except:
		return  []


def index_page_parser(url_index):
	page, status_code = urllib3_request(url_index)
	url_html = ''
	interactiveDataBtn = ''
	if status_code == 200:
		### get the url of the html
		tree = html.fromstring(page)
		#href = tree.xpath('//td[text() = "10-K"]/preceding-sibling::td/a/@href') ### hard coded form_type "10-K"
		href = tree.xpath('//td/a/@href')
		url_html = 'https://www.sec.gov' + href[0]
		### get the url of the interactiveDataBtn
		href = tree.xpath('//a[@id="interactiveDataBtn"]/@href')
		if len(href)>0:
			interactiveDataBtn = 'https://www.sec.gov' + href[0]
		'''			
		else:
			hrefs = tree.xpath('//a/@href')
			if len(hrefs)>0:
				href = [h for h in hrefs if '/cgi-bin/viewer?action=view' in h]
				self.interactiveDataBtn = 'https://www.sec.gov' + href[0]			
			else:
				self.interactiveDataBtn = 'https://www.sec.gov' + '/cgi-bin/viewer?action=view&amp;cik=' \
					+ self.cik + '&amp;accession_number=' + self.accession + '&amp;xbrl_type=v'
		'''
	else:
		print('Failed to get .HTML CorpFiling URL', status_code)
	return 	url_html, interactiveDataBtn



def period_of_report_parser(url_index):
	page, status_code = urllib3_request(url_index)
	period_of_report = ''
	if status_code == 200:
		tree = html.fromstring(page)
		try:
			period = tree.xpath('//div[text() = "Period of Report"]/following-sibling::div/text()')
			period_of_report = period[0]
		except Exception as e:
			print(e) 
	else:
		print('Failed to get .HTML CorpFiling URL', status_code)
	return period_of_report




def interactiveDataBtn_page_parser(url_interactiveDataBtn):
	page, status_code = urllib3_request(url_interactiveDataBtn)
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

def urllib3_request(input_url, verbose=False):
	page = []
	status_code = []
	pool = urllib3.PoolManager(cert_reqs='CERT_REQUIRED',ca_certs=certifi.where())
	try:
		r = pool.request('GET', input_url)
		status_code = r.status
		page = r.data
		r.close()
	except:
		if verbose:
			print('urllib3_request failed to open URL:' + input_url)
	return page, status_code

def remove_tags(text):
	tree = html.fromstring(text)
	text = tree.xpath("//text()")
	text = ''.join(text)
	text = text.replace("\n","")
	text = text.replace("/s/","")
	text = text.replace(u'\xa0',u"")
	return text
