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




