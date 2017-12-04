from Edgar import *


for year in np.arange(2014,2018):
	
	with open("../results/filings13F_"+str(year)+".pkl",'rb') as f:
		ci13F = pickle.load(f)

	ci13FHR = ci13F.loc[ci13F.form_type.str.contains("HR")]
	cf = CorpFiling(ci13FHR)

	## parse the tables 
	# the table_13F_parser() only supports year 2014-present
	# before 2014, tables are in text files instead of html files.
	cf.table_13F_parser()
	print(year, len(cf.tables_13F))
	with open("../results/table-13F-"+str(year)+".pkl",'wb') as f:
		pickle.dump( cf.tables_13F, f)
		cf.tables_13F.to_csv("../results/tables-13F-"+str(year)+".csv", sep='\t')
