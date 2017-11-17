
def create_interactive_url_dict(keys_cat1,keys_all,vals):
	interactiveURL_dict = {}
	### create level-1 dict
	j = 0
	k = 0
	for i in range(len(keys_all)):
		if keys_all[i] in keys_cat1:
			j = i
			interactiveURL_dict[keys_all[j]] = {}
		else:
			interactiveURL_dict[keys_all[j]][keys_all[i]] = vals[k]
			k = k + 1
	return str(interactiveURL_dict)


def getCIK_from_Ticker():
	# a lookup between CIF and stock symbol ticker
	# input a stock symbol or company name and return its CIK number
	pass




