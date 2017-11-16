from edgar import *


def main():

	###load company_filing DataBase
	db_filing_dir = '/'.join(master_index.index_save_dir.split('/')[0:-2]) + '/'
	db_filing_fname = db_filing_dir + 'db_company_filing.pkl'
	if os.path.isfile(db_filing_fname) == True:
		print('loading master_index from database')
		with open(db_filing_fname, "rb") as f:
			db_filing = pickle.load(f)
	else:
		db_filing = pd.DataFrame(columns = company_list.columns)


	##############################################################
	#######     download filing .htm/.txt/ interactive   #########
	##############################################################
	company_list = db_filing

	###create a connection pool to SEC
	pool = urllib3.connectionpool.HTTPSConnectionPool( "www.sec.gov" , maxsize = 4,
			cert_reqs='CERT_REQUIRED',ca_certs=certifi.where(), )
	for i  in range(len(company_list)):
		company = company_list.iloc[i]
		print(company.cik + '\t' + company.company_name + '\t' + company.date_filed)

		###download html/txt files
		web = webDownloader(pool, company.url_html)
		web.download()

		###if interactiveURLXBRL exists, download interactive files
		if len( company.url_IntActDict ) > 0:
			url_IntActDict = eval(company.url_IntActDict)
			try:
				for cat1_key in url_IntActDict.keys():
					for cat2_key in url_IntActDict[cat1_key]:
						input_url = url_IntActDict[cat1_key][cat2_key]
						web = webDownloader(pool, input_url)
						web.download()
			except:
				pass


if __name__ == '__main__':
	main()

