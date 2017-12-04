
import numpy as np
import pandas as pd
import re
import edgar
import time

from lxml import etree, html
import requests


def CreateDict_interactiveDB(cat1_keys,keys,vals):
	InteractiveDB_Dict = {}
	### create level-1 dict
	j = 0
	k = 0
	for i in range(len(keys)):
		if keys[i] in cat1_keys:
			j = i
			InteractiveDB_Dict[keys[j]] = {}
		else:
			InteractiveDB_Dict[keys[j]][keys[i]] = vals[k]
			k = k + 1
	return InteractiveDB_Dict


url = 'https://www.sec.gov/Archives/edgar/data/320193/000162828016020309/0001628280-16-020309-index.htm'
#url = 'https://www.sec.gov/Archives/edgar/data/1000032/000120919116104477/0001209191-16-104477-index.htm'
t = time.time()

page, status_code = edgar.urllib3_request(url)
tree = html.fromstring(page)
#page = requests.get(url)
#print(page.status_code)
#tree = html.fromstring(page.content)

elapsed = time.time() - t
print(elapsed)


t = time.time()

hrefs = tree.xpath('//td/a/@href')
print(hrefs[0])

href = tree.xpath('//td[text() = "10-K"]/preceding-sibling::td/a/@href')
if len(href)>0:
	print(href[0])
else:
	print('can not find this url')

href = tree.xpath('//a[@id="interactiveDataBtn"]/@href')
if len(href)>0:
	print(href[0])
	url_interactiveDataBtn = "https://www.sec.gov" + href[0]
else:
	url_interactiveDataBtn = []


hrefs = tree.xpath('//a/@href')
href = [h for h in hrefs if '/cgi-bin/viewer?action=view' in h]
if len(href)>0:
	print(href[0])


####
page, status_code = edgar.urllib3_request(url_interactiveDataBtn)
if len(page)>0:
	tree = html.fromstring(page)

	#javascript_text = tree.xpath('//script[@type="text/javascript" and @language="javascript"]/text()')
	javascript_text = tree.xpath('//script[@type="text/javascript"]/text()')
	print(len(javascript_text))
	
	urls = [line.split('=')[-1].replace('"','') for line in javascript_text[0].split(';') if '/Archives/edgar/data/' in line and '.htm' in line]
	for el in urls:
		print(el)

	cat1 = tree.xpath('//ul[@id = "menu"]/child::li[@class="accordion"]/a/text()')
	for el in cat1:
		print(el)

	menu_text = tree.xpath('//ul[@id = "menu"]/descendant::li[@class="accordion"]/a/text()')
	for el in menu_text:
		print(el)
#	menu_cat_href = tree.xpath('//ul[@id = "menu"]/descendant::li[@class="accordion"]/a/@href')
#	for el in menu_cat_href:
#		print(el)

	print(len(menu_text), len(urls))

	InteractiveDB_Dict = CreateDict_interactiveDB(cat1,menu_text,urls)
	for cat1_key in InteractiveDB_Dict.keys():
		for cat2_key in InteractiveDB_Dict[cat1_key]:
			#if "note" in cat1_key.lower() or "note" in cat2_key.lower(): 
			print(cat1_key, cat2_key, InteractiveDB_Dict[cat1_key][cat2_key])




