import urllib3
import shutil
import certifi
from lxml import etree, html



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
