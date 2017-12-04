from Edgar import *


## parse the urls from the index page
for year in np.arange(2017,1992,-1):
	
	mi = MasterIndex(str(year),'1:4')

	ci13F = mi.corpidx[mi.corpidx.form_type.str.contains('13F')]

	cf = CorpFiling(ci13F)

	# parse the urls
	cf.url_parser()

	print(cf.corpidx.head())
	print(cf.corpidx.shape)

	with open("../results/filings13F_"+str(year)+".pkl",'wb') as f:
		pickle.dump( cf.corpidx, f)
	cf.corpidx.to_csv("../results/filings13F_"+str(year)+".csv", sep='\t')


## download filing files from the parsed urls
for year in np.arange(2017,1992,-1):
	
	with open("../results/filings13F_"+str(year)+".pkl",'rb') as f:
		ci13F = pickle.load(f)

	ci13FHR = ci13F.loc[ci13F.form_type.str.contains("HR")]
	cf = CorpFiling(ci13FHR)
	cf.filing_download(verbose=True)



'''
## parse the period_of_report information
for year in np.arange(2017,1992,-1):
	print("parse the period_of_report, year:", year)

	with open("../results/filings13F_"+str(year)+".pkl",'rb') as f:
		ci13F = pickle.load(f)

	cf = CorpFiling(ci13F)

	# prase the period of report
	cf.period_of_report_parser()

	print(cf.corpidx.head())
	print(cf.corpidx.shape)

	with open("../results/filings13F_"+str(year)+".pkl",'wb') as f:
		pickle.dump( cf.corpidx, f)
	cf.corpidx.to_csv("../results/filings13F_"+str(year)+".csv", sep='\t')
'''
