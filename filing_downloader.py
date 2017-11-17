import os
import edgar
import urllib3
import certifi
import pandas as pd
import numpy as np


def main():
	filings_list_filename = '../filings10K.pkl'

	if os.path.isfile(filings_list_filename) == False:
		print(filings_list_filename + ' file not found')

	else:
		with open(filings_list_filename, "rb") as f:
			company_list = pickle.load(f)
		edgar.download_filings(company_list)

if __name__ == '__main__':
	main()

