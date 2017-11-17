from edgar2 import *

mi = MasterIndex('1993:2017','1:4')

corp_index_10K = mi.corp_index[mi.corp_index.form_type.str.contains('10K')]

print(corp_index_10K.head(50))

cf = CorpFiling(corp_index_10K)

cf.url_parser()

print(cf.corp_index.head())

with open("../corpfiling10K.pkl",'wb') as f:
	pickle.dump(f)

cf.filing_download(verbose=True)
