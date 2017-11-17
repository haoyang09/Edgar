import edgar


def main():

	# input
	year_range 			= '1993:2017'
	qtr_range  			= '1:4'
	form_type 			= ['10-K','10-K/A']
	filings_save_dir = '../filings10K.pkl'

	col_name_list = ['accession', 'url_index', 'url_html', 'url_txt', 'interactiveDataBtn', 'interactiveDataDict']

	# get the master index
	mi = edgar.MasterIndex(year_range,qtr_range)
	master_index = mi.index_data
	
	'''
	master_index_file 	= '../IndexFiles/master_index.pkl'
	if isfile(master_index_file):
		with open(master_index_file, 'rb') as f:
		master_index_data = pickle.load(f)
	else:
		mi = edgar.MasterIndex(year_range,qtr_range)
		master_index_data = mi.index_data
		with open(master_index_file, 'wb') as f:
			pickle.dump(master_index_data, f)
	'''

	# load/update company_list url  database  
	# get company_list for the specified form_types 
	# company_list = master_index_data.loc[lambda df: df.form_type.isin(form_type), :]
	# get company_list for the specified year, quarter
	# company_list = company_list.loc[lambda df: df.date_filed.str.contains('2016'), :]
	company_list_index = []
	for ft in form_type:
		company_list_index += master_index_data[lambda df: df.form_type.str.contains(ft)].index.tolist()
	if len(company_list_index) > 0:
		print(str(len(company_list_index)) + ' filings found for form_type ' + ', '.join(s for s in form_type))
		company_list = master_index_data.iloc[company_list_index]
		company_list = company_list.set_index(np.arange(0,len(company_list))) 
		for col_name in col_name_list:
			company_list.loc[:,col_name] 	= ''
	else:
		company_list = pd.DataFrame(columns = master_index_data.columns.tolist() + col_name_list)

	if os.path.isfile(filings_save_dir) == True:
		with open(filings_save_dir, "rb") as f:
			filings10K = pickle.load(f)
	else:
		filings10K = pd.DataFrame(columns = company_list.columns)

	# parse company_list of 10K
	edgar.filingParser(company_list)

	# test parsing result 
	with open(filings_save_dir,'rb') as f:
		db = pickle.load(f)
	print(db.head())
	print(db[db.cik=='1099160'])


if __name__ == "__main__":
	main()


