from Edgar import *


## download filing files from the parsed urls
for year in np.arange(2014,1992,-1):
	
	with open("../results/filings13F_"+str(year)+".pkl",'rb') as f:
		ci13F = pickle.load(f)

	#ci13FHR = ci13F.loc[ci13F.form_type.str.contains("HR")]
	cf = CorpFiling(ci13F)
	cf.filing_download(verbose=True)
