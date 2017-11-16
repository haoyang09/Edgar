import argparse
import os
import sys
import csv
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.tag import pos_tag
from nltk.stem import WordNetLemmatizer
from nltk.probability import FreqDist

def main():

    parser = argparse.ArgumentParser()

    parser.add_argument('-t', '--target', help = 'The targeted item. Tips: --target item_1 item_1a', nargs = '+')

    args = parser.parse_args()


def traverse_target(target):

    for dirpath, dirnames, filenames in os.walk(os.path.abspath('.')):

        for filename in filenames:

            if os.path.splitext(filename)[1] == '.idx':

                if 'item_1' in target :

                    if os.path.splitext(filename)[0] + '_ITEM_1' in dirnames:
                        if os.path.exists(os.path.join(os.path.abspath('.'), 'ITEM_1_Temp.txt')):
                            os.remove(os.path.join(os.path.abspath('.'), 'ITEM_1_Temp.txt'))
                            output_txt = open(os.path.join(os.path.abspath('.'), 'ITEM_1_Temp.txt'), 'w')
                        else:
                            output_txt = open(os.path.join(os.path.abspath('.'), 'ITEM_1_Temp.txt'), 'w')

                        if os.path.exists(os.path.join(os.path.abspath('.'), 'ITEM_1_List.csv')):
                            os.remove(os.path.join(os.path.abspath('.'), 'ITEM_1_List.csv'))
                            list_txt = csv.writer(open(os.path.join(os.path.abspath('.'), 'ITEM_1_List.csv'), 'wb'), quoting = csv.QUOTE_ALL)
                            list_txt.writerow(['COMPANY NAME: ', 'CIK: ', 'SIC: ', 'FILING DATE: ', 'FILE NAME: '])
                        else:
                            list_txt = csv.writer(open(os.path.join(os.path.abspath('.'), 'ITEM_1_List.csv'), 'wb'), quoting = csv.QUOTE_ALL)
                            list_txt.writerow(['COMPANY NAME: ', 'CIK: ', 'SIC: ', 'FILING DATE: ', 'FILE NAME: '])

                        original_dir = os.path.join(os.path.abspath('.'), os.path.splitext(filename)[0] + '_ITEM_1')
                        traverse_folder(original_dir, output_txt, list_txt)
                        output_txt.close()

                        if os.path.exists(os.path.join(os.path.abspath('.'), 'ITEM_1.txt')):
                            os.remove(os.path.join(os.path.abspath('.'), 'ITEM_1.txt'))
                            final_output_txt = open(os.path.join(os.path.abspath('.'), 'ITEM_1.txt'), 'w')
                        else:
                            final_output_txt = open(os.path.join(os.path.abspath('.'), 'ITEM_1.txt'), 'w')
                        temp_txt = open(os.path.join(os.path.abspath('.'), 'ITEM_1_Temp.txt'), 'r')
                        LDA_preprocess(temp_txt, final_output_txt)
                        temp_txt.close()
                        os.remove(os.path.abspath('.'), 'ITEM_1_Temp.txt')
                    else:
                        sys.exit('Directory %s can not be found' % os.path.splitext(filename)[0] + '_ITEM_1')

                if 'item_1a' in target:

                    if os.path.splitext(filename)[0] + '_ITEM_1A' in dirnames:
                        if os.path.exists(os.path.join(os.path.abspath('.'), 'ITEM_1A_Temp.txt')):
                            os.remove(os.path.join(os.path.abspath('.'), 'ITEM_1A_Temp.txt'))
                            output_txt = open(os.path.join(os.path.abspath('.'), 'ITEM_1A_Temp.txt'), 'w')
                        else:
                            output_txt = open(os.path.join(os.path.abspath('.'), 'ITEM_1A_Temp.txt'), 'w')

                        if os.path.exists(os.path.join(os.path.abspath('.'), 'ITEM_1A_List.csv')):
                            os.remove(os.path.join(os.path.abspath('.'), 'ITEM_1A_List.csv'))
                            list_txt = csv.writer(open(os.path.join(os.path.abspath('.'), 'ITEM_1A_List.csv'), 'wb'), quoting = csv.QUOTE_ALL)
                            list_txt.writerow(['COMPANY NAME: ', 'CIK: ', 'SIC: ', 'FILING DATE: ', 'FILE NAME: '])
                        else:
                            list_txt = csv.writer(open(os.path.join(os.path.abspath('.'), 'ITEM_1A_List.csv'), 'wb'), quoting = csv.QUOTE_ALL)
                            list_txt.writerow(['COMPANY NAME: ', 'CIK: ', 'SIC: ', 'FILING DATE: ', 'FILE NAME: '])

                        original_dir = os.path.join(os.path.abspath('.'), os.path.splitext(filename)[0] + '_ITEM_1A')
                        traverse_folder(original_dir, output_txt, list_txt)
                        output_txt.close()

                        if os.path.exists(os.path.join(os.path.abspath('.'), 'ITEM_1A.txt')):
                            os.remove(os.path.join(os.path.abspath('.'), 'ITEM_1A.txt'))
                            final_output_txt = open(os.path.join(os.path.abspath('.'), 'ITEM_1A.txt'), 'w')
                        else:
                            final_output_txt = open(os.path.join(os.path.abspath('.'), 'ITEM_1A.txt'), 'w')
                        temp_txt = open(os.path.join(os.path.abspath('.'), 'ITEM_1A_Temp.txt'), 'r')
                        LDA_preprocess(temp_txt, final_output_txt)
                        temp_txt.close()
                        os.remove(os.path.abspath('.'), 'ITEM_1A_Temp.txt')
                    else:
                        sys.exit('Directory %s can not be found' % os.path.splitext(filename)[0] + '_ITEM_1A')

def traverse_folder(original_dir, output_txt, list_txt):

    original_folders = os.listdir(original_dir)

    count = 0

    for f in original_folders:

        if f == '.DS_Store' or os.path.splitext(f)[1].lower() == '.swp':
            continue

        elif os.path.splitext(f)[1].lower() == '.pdf' or os.path.splitext(f)[1].lower() == '.xml' or os.path.splitext(f)[1].lower() == '.paper':
            continue

        elif os.path.splitext(f)[1] == '.txt':
            count += 1
            print f
            print 'NLP: ' + str(count) + ' of ' + str(len(original_folders))

            NLP(original_dir, f, output_txt, list_txt)

        else:
            print 'Processing ' + f + ' ...'

            if not os.path.isdir(os.path.join(original_dir, f)):
                os.mkdir(os.path.join(original_dir, f))

            traverse_folder(os.path.join(original_dir, f), output_txt, list_txt)

def NLP(original_dir, f, output_txt, list_txt):

    original_file = open(os.path.join(original_dir, f))

    # Get company name
    original_content = original_file.readline()
    company_name = original_content.lstrip('COMPANY NAME: ').rstrip('\n')

    # Get CIK
    original_content = original_file.readline()
    cik = original_content.lstrip('CIK: ').rstrip('\n')

    # Get SIC
    original_content = original_file.readline()
    sic = original_content.lstrip('SIC: ').rstrip('\n')

    # Get filing date
    original_content = original_file.readline()
    filing_date = original_content.lstrip('FILING DATE: ').rstrip('\n')

    # Split original content by space
    splited_content = []
    original_content = original_file.readline()

    while original_content != '':

        for i in original_content.split():

            splited_content.append(i)

        original_content = original_file.readline()

    # Tokenize content
    tokenized_content = []

    for sc in splited_content:

        try:
            sc = sc.lower().replace('\xaa', '').replace('\xab', '').replace('\xac', '').replace('\xad', '').replace('\xae', '').replace('\xaf', '').replace('\xa0', '').replace('\xa1', '').replace('\xa2', '').replace('\xa3', '').replace('\xa4', '').replace('\xa5', '').replace('\xa6', '').replace('\xa7', '').replace('\xa8', '').replace('\xa9', '').replace('\xba', '').replace('\xbb', '').replace('\xbc', '').replace('\xbe', '').replace('\xbf', '').replace('\xb0', '').replace('\xb1', '').replace('\xb2', '').replace('\xb3', '').replace('\xb4', '').replace('\xb5', '').replace('\xb6', '').replace('\xb7', '').replace('\xb8', '').replace('\xb9', '').replace('\xca', '').replace('\xcb', '').replace('\xcc', '').replace('\xcd', '').replace('\xce', '').replace('\xcf', '').replace('\xc0', '').replace('\xc1', '').replace('\xc2', '').replace('\xc3', '').replace('\xc4', '').replace('\xc5', '').replace('\xc6', '').replace('\xc7', '').replace('\xc8', '').replace('\xc9', '').replace('\xea', '').replace('\xeb', '').replace('\xec', '').replace('\xed', '').replace('\xee', '').replace('\xef', '').replace('\xe0', '').replace('\xe1', '').replace('\xe2', '').replace('\xe3', '').replace('\xe4', '').replace('\xe5', '').replace('\xe6', '').replace('\xe7', '').replace('\xe8', '').replace('\xe9', '').replace('\x80', '').replace('\x81', '').replace('\x82', '').replace('\x83', '').replace('\x84', '').replace('\x85', '').replace('\x86', '').replace('\x87', '').replace('\x88', '').replace('\x89', '').replace('\x8a', '').replace('\x8b', '').replace('\x8c', '').replace('\x8d', '').replace('\x8e', '').replace('\x8f', '').replace('\x90', '').replace('\x91', '').replace('\x92', '').replace('\x93', '').replace('\x94', '').replace('\x95', '').replace('\x96', '').replace('\x97', '').replace('\x98', '').replace('\x99', '').replace('\x9a', '').replace('\x9b', '').replace('\x9c', '').replace('\x9d', '').replace('\x9e', '').replace('\x9f', '').encode('utf-8')
		
        except:
            continue

        for wt in word_tokenize(sc):

            tokenized_content.append(wt)
    # Remove stopwords
    non_stopwords_content = []

    english_stopwords = stopwords.words('english')

    for tc in tokenized_content:

        if tc.lower() not in english_stopwords:

            non_stopwords_content.append(tc)

    # Remove punctuation
    english_punctuation = [',', '<', '.', '>', '/', '?', ';', ':', "'", '"', '[', '{', ']', '}', '`', '~', '!', '@', '#', '$', '%', '^', '&', '*', '(', ')', '-', '_', '=', '+']

    non_punctuation_content = []

    for ns in non_stopwords_content:

        if ns not in english_punctuation:

            non_punctuation_content.append(ns)

    # Tag words
    tagged_content = pos_tag(non_punctuation_content)
    # Lemmatize words
    lemmatizer = WordNetLemmatizer()

    lemmatized_content = []

    for tc in tagged_content:

        if tc[1] in ['NN', 'NNS', 'NNP', 'NNPS']:

            lemmatized_content.append(lemmatizer.lemmatize(tc[0], pos = 'n'))

    # Remove the files which the words less than 10
    if(len(lemmatized_content) > 10):

        count = 1

        for fc in lemmatized_content:

            if count != len(lemmatized_content):
                output_txt.write(fc + ' ')
                count += 1
            else:
                output_txt.write(fc + '\n')

        # Get file name
        file_name = os.path.splitext(f)[0]

        list_input = [company_name, cik, sic, filing_date, file_name]

        list_txt.writerow(list_input)

def LDA_preprocess(temp_txt, final_output_txt):

    temp_original = []

    temp_splited = []
    # Store the original content and get all the words in original content
    content_line = temp_txt.readline()

    while content_line != '':

        temp_original.append(content_line)

        for word in content_line.split():

            temp_splited.append(word)

        content_line = temp_txt.readline()

    print len(temp_splited)

    print len(temp_original)

    final_output_txt.write(str(len(temp_original)) + '\n')

    final_words = []

    print FreqDist(temp_splited)

    print FreqDist(temp_splited).most_common(10)
    # Remove the words which are in high frequency(The highest 10)
    top_10_freq = []

    for i in FreqDist(temp_splited).most_common(10):

        top_10_freq.append(i[0])
    # Remove the words which are in low frequency(lower than 5)
    for ts in FreqDist(temp_splited).iteritems():

        if ts[1] > 5 and ts[0] not in top_10_freq:

            final_words.append(ts[0])

    i = 0
    # For each line in original content, remove the words which are not in high frequency words
    for to in temp_original:

        i += 1

        print 'LDA preprocessing: ' + str(i) + ' of ' + str(len(temp_original))

        count = 1

        for word in to.split():

            if count != len(to.split()):

                if word in final_words:

                    final_output_txt.write(word + ' ')

                    count += 1

                else:

                    count += 1
            else:

                if word in final_words:

                    final_output_txt.write(word + '\n')

                else:

                    final_output_txt.write('\n')


if __name__ == '__main__':
main(sys.argv[1:])
