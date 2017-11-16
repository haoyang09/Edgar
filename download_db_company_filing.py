from edgar import *

def main():

	'''load/update master_index  database'''
	''' year and quarter range'''
	year_range = '2003:2016'
	qtr_range  = '1:4'
	### form types to perform search in
	form_type = ['10-K','10-K/A']
	m_index = master_index(year_range,qtr_range)
	master_index_data = m_index.index_data


	'''load/update company_list url  database  '''
	''' get company_list for the specified form_types '''
	company_list = master_index_data.loc[lambda df: df.form_type.isin(form_type), :]
	''' get company_list for the specified year, quarter'''
	#company_list = company_list.loc[lambda df: df.date_filed.str.contains('2016'), :]
	''' reset index values ''' 
	company_list = company_list.set_index([range(0,len(company_list))]) 
	print(str(len(company_list)) + ' filings found for form_type' + ', '.join(s for s in form_type))
	
	company_list.loc[:,'accession'] 	= ''
	company_list.loc[:,'url_index'] 	= ''
	company_list.loc[:,'url_html'] 		= ''
	company_list.loc[:,'url_txt'] 		= ''
	company_list.loc[:,'url_IntActBtn'] 	= ''
	company_list.loc[:,'url_IntActDict']	= ''
	
	''' load company_filing DataBase'''
	db_filing_dir = '/'.join(master_index.index_save_dir.split('/')[0:-2]) + '/'
	db_filing_fname = db_filing_dir + 'db_company_filing.pkl'
	if os.path.isfile(db_filing_fname) == True:
		print('loading master_index from database')
		with open(db_filing_fname, "rb") as f:
			db_filing = pickle.load(f)
	else:
		db_filing = pd.DataFrame(columns = company_list.columns)

	update_count = 0
	company_list_len =  len(company_list)
	for i in range(company_list_len):

		company_index = company_list.iloc[i]
		''' create an instance of class edgar_filing '''
		f1 = company_filing(company_index)

		''' use accession number to check if record exists in db_filing '''
		db_index = db_filing[ db_filing.file_name == f1.file_name ].index.tolist()
		print(str(i) + ': ' + company_index['cik'] + '\t' + company_index['date_filed'])

		if len(db_index) == 0:
			''' if record not exist in db, parsing SEC and update database '''
			print('url not found in database, parse SEC to get urls')
			f1.FilingURLPraser()
			f1.IntActURLParser()
			company_index.accession 		= f1.accession
			company_index.url_index 		= f1.url_index
			company_index.url_html 			= f1.url_html
			company_index.url_txt 			= f1.url_txt
			company_index.url_IntActBtn 		= f1.url_IntActBtn
			company_index.url_IntActDict 		= f1.url_IntActDict

			db_filing = db_filing.append(company_index, ignore_index=True)
			print('database length updated: ' + str(len(db_filing)))

		elif (  len(db_filing.loc[db_index[0],'url_IntActBtn'])  > 3 )  and \
		     (  len(db_filing.loc[db_index[0],'url_IntActDict']) < 3 ) :
			''' update Interactive URL '''
			f1.FilingURLPraser()
			f1.IntActURLParser()
			db_filing.loc[db_index, 'url_IntActBtn']  = f1.url_IntActBtn
			db_filing.loc[db_index, 'url_IntActDict'] = f1.url_IntActDict
			print('interactive URLs: ', db_filing.loc[db_index[0],'url_IntActDict'])
			update_count = update_count + 1
		else:
			print('download skipped')

		''' save the db_filing update to file every 5000 entries'''
		if ((i+1) % 5000 == 0) or (i == company_list_len):
			print('save database to file')
			''' reset index values 	'''
			db_filing = db_filing.set_index([range(0,len(db_filing))])
			with open(db_filing_fname, "wb") as f:
				pickle.dump(db_filing, f)
	
	print('update_count: ' + str(update_count))

if __name__ == '__main__':
	main()
