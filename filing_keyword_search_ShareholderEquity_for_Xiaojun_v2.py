from edgar import *
from shutil import copyfile

menu_key1 = 'financial statements'
menu_key2 = 'equit'


dst_dir = '/home/haoyang/Dropbox/JeanHao/Edgar/ForXiaojun/'


''' load company_filing DataBase '''
company_list_dir = '/'.join(master_index.index_save_dir.split('/')[0:-2]) + '/'
company_list_fname = company_list_dir + 'db_company_filing.pkl'
if os.path.isfile(company_list_fname) == True:
	print('loading master_index from database')
	with open(company_list_fname, "rb") as f:
		company_list_db = pickle.load(f)
else:
	company_list_db = pd.DataFrame(columns = company_list.columns)

#company_list = company_list_db[ company_list_db['cik'] == '320193' ]
#company_list = company_list_db[ company_list_db['accession'] == '0001193125-09-214859' ] 
#company_list = company_list_db.iloc[-5000:]
company_list = company_list_db
#print(company_list)

print(company_list.columns)

''' initialize the search_result pd dataframe '''
#search_result =  pd.DataFrame(columns = output_columns)
output_columns = ['cik','company_name','form_type','date_filed','url_index','url_html']
search_result = company_list[output_columns].copy()
search_result = search_result.set_index([range(0,len(search_result))])
search_result['url_interactive'] = ''

SAVE_FILES = False

''' loop through the company_list and perform keyword search'''
for i  in range(len(company_list)):
	company = company_list.iloc[i]
	#print(company.cik + '\t' + company.company_name + '\t' + company.date_filed)

	### if interactiveURLXBRL doesn't exist
	if len( company.url_IntActDict ) == 0:
		input_url = company.url_html
		if SAVE_FILES: 
			web = webDownloader([], input_url)
			print(web.local_file_dir)
			#page = web.read() #web.readlines()
			#text = remove_tags(page)
			copy_path = dst_dir + company.date_filed[:4]+'/'
			if not os.path.exists(copy_path): os.makedirs(copy_path)		
			copyfile(web.local_file_dir, copy_path+company.cik+'.htm')
		
	### if interactiveURLXBRL exists
	else:
		###company.url_IntActDict is a dict converted to string
		### use eval() to convert back to dict
		url_IntActDict = eval(company.url_IntActDict)
		for cat1_key in url_IntActDict.keys():
			for cat2_key in url_IntActDict[cat1_key]:
				if (menu_key1 == cat1_key.lower()) and (menu_key2 in cat2_key.lower()):
					#print(company.company_name, cat1_key, cat2_key)
					###read file content from given URL
					input_url = url_IntActDict[cat1_key][cat2_key]
					search_result.loc[i]['url_interactive'] = "https://www.sec.gov"+input_url
					if SAVE_FILES: 
						web = webDownloader([], input_url)
						#print(web.local_file_dir)
						#page = web.read()
						#text = remove_tags(page)
						copy_path = dst_dir + company.date_filed[:4]+'/'
						if not os.path.exists(copy_path): os.makedirs(copy_path)		
						copyfile(web.local_file_dir, copy_path+company.cik+'.htm')
					

search_result=search_result.rename(columns = {'url_html':'url_full_10K'})
''' output seaerch_result to file '''
search_result_fname = '/home/haoyang/Dropbox/JeanHao/Edgar/' + 'search_result_for_Xiaojun_'+ time.strftime("%Y%m%d")+ '.csv'
search_result.to_csv(search_result_fname, sep='\t',encoding = 'utf-8')
print('saved to file:', search_result_fname)
print(search_result.head())




