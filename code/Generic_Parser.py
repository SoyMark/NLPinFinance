"""
Program to provide generic parsing for all files in user-specified directory.
The program assumes the input files have been scrubbed,
  i.e., HTML, ASCII-encoded binary, and any other embedded document structures that are not
  intended to be analyzed have been deleted from the file.

Dependencies:
    Python:  Load_MasterDictionary.py
    Data:    LoughranMcDonald_MasterDictionary_2014.csv

The program outputs:
   1.  File name
   2.  File size (in bytes)
   3.  Number of words (based on LM_MasterDictionary
   4.  Proportion of positive words (use with care - see LM, JAR 2016)
   5.  Proportion of negative words
   6.  Proportion of uncertainty words
   7.  Proportion of litigious words
   8.  Proportion of modal-weak words
   9.  Proportion of modal-moderate words
  10.  Proportion of modal-strong words
  11.  Proportion of constraining words (see Bodnaruk, Loughran and McDonald, JFQA 2015)
  12.  Number of alphanumeric characters (a-z, A-Z, 0-9)
  13.  Number of alphabetic characters (a-z, A-Z)
  14.  Number of digits (0-9)
  15.  Number of numbers (collections of digits)
  16.  Average number of syllables
  17.  Averageg word length
  18.  Vocabulary (see Loughran-McDonald, JF, 2015)

  ND-SRAF
  McDonald 2016/06
"""

import csv
import glob
import os
import re
import string
import sys
import time
#sys.path.append('D:\GD\Python\TextualAnalysis\Modules')  # Modify to identify path for custom modules
import Load_MasterDictionary as LM
import numpy as np
from tqdm import tqdm
import math
import multiprocessing as mp

"""
    Specify File Locations for Generic Parser.py
"""

# User defined directory for files to be parsed
TARGET_FILES = r'./data/*/*/*.txt'

# User defined file pointer to LM dictionary
MASTER_DICTIONARY_FILE = r'./LoughranMcDonald_MasterDictionary_2014.csv'
HARVARD_NEG_FILE = r'./Harvard IV_Negative Word List_Inf.txt'

EXP_SETTING = "LM"
# EXP_SETTING = "Harvard"
assert EXP_SETTING in ["LM", "Harvard"]

# # User defined output file
# OUTPUT_FILE = r'./result2014-2016.csv'

# # Setup output
# OUTPUT_FIELDS = ['filename', 'file size', 'number of words', '% positive', '% negative',
#                  '% uncertainty', '% litigious', '% modal-weak', '% modal moderate',
#                  '% modal strong', '% constraining', '# of alphanumeric', '# of digits',
#                  '# of numbers', 'avg # of syllables per word', 'average word length', 'vocabulary',
#                  'CIK', ]

lm_dictionary = LM.load_masterdictionary(MASTER_DICTIONARY_FILE, True)
with open(HARVARD_NEG_FILE, 'r') as f:
    harvard_neg_words = {line.strip().upper() for line in f if line.strip()}

if EXP_SETTING == "LM":
    neg_words = [word for word in lm_dictionary if lm_dictionary[word].negative]
else:
    neg_words = harvard_neg_words
neg_words_idx = {}
cnt = 0
for word in neg_words:
    neg_words_idx[word] = cnt
    cnt += 1

tf_matrix = [] # (# of documents, # of negative words in lm_dictionary)
idf_matrix = [] # (# of documents, # of negative words in lm_dictionary)
doc_length_matrix = [] # (# of documents, 1).


def processing_doc(doc):
    doc_length = 0
    tf_line = [0] * len(neg_words)
    idf_line = [0] * len(neg_words)
    tokens = re.findall('\w+', doc)  # Note that \w+ splits hyphenated words
    for token in tokens:
        if not token.isdigit() and len(token) > 1 and token in lm_dictionary:
            doc_length += 1
            if token in neg_words_idx:
                tf_line[neg_words_idx[token]] += 1
                idf_line[neg_words_idx[token]] = 1
    return tf_line, idf_line, doc_length

def extract_cik_from_filename(filename):
    parts = filename.split('_')
    if len(parts) >= 5:
        accession = parts[4]
        cik = accession.split('-')[0]
        return cik
    return None

def extract_date_from_filename(filename):
    number_date = filename.split('_')[0]
    return f"{number_date[:4]}-{number_date[4:6]}-{number_date[6:8]}"

def process_single_file(filename):
    """Process a single file and return results for parallel execution."""
    try:
        with open(filename, 'r', encoding='UTF-8', errors='ignore') as f_in:
            doc = f_in.read()
        doc = re.sub('(May|MAY)', ' ', doc)  # drop all May month references
        doc = doc.upper()  # for this parse caps aren't informative so shift

        tf_line, idf_line, doc_length = processing_doc(doc)
        fname = os.path.basename(filename)
        cik = extract_cik_from_filename(fname)
        return {
            'tf_line': tf_line,
            'idf_line': idf_line,
            'doc_length': doc_length,
            'filename': fname,
            'cik': cik,
        }
    except Exception as e:
        print(f"Error processing {filename}: {e}")
        return None


def process():

    file_list = glob.glob(TARGET_FILES)
    print(f"Total files to process: {len(file_list)}")
    print(f"First few files: {file_list[:3]}")

    # Determine the number of processes (use CPU count or a fixed number)
    num_processes = mp.cpu_count()
    print(f"Using {num_processes} processes")

    # Create a process pool
    with mp.Pool(processes=num_processes) as pool:
        # Map the file processing to the pool
        results = []
        for result in tqdm(pool.imap_unordered(process_single_file, file_list), total=len(file_list)):
            if result is not None:
                results.append(result)

    # Collect results into matrices
    for result in results:
        tf_matrix.append(result['tf_line'])
        idf_matrix.append(result['idf_line'])
        doc_length_matrix.append(result['doc_length'])

    tf_matrix_np = np.array(tf_matrix, dtype=float)
    idf_matrix_np = np.array(idf_matrix)
    doc_length_matrix_np = np.array(doc_length_matrix, dtype=float).reshape(-1, 1)
    
    # tf
    tf_matrix_normalized = np.zeros_like(tf_matrix_np)
    for i in range(len(doc_length_matrix)):
        if doc_length_matrix_np[i, 0] > 0:
            tf_matrix_normalized[i, :] = tf_matrix_np[i, :] / doc_length_matrix_np[i, 0]
    # idf
    num_docs = len(file_list)
    word_doc_counts = np.sum(idf_matrix_np, axis=0) 
    idf_vector = np.array([math.log(num_docs / (count + 1)) for count in word_doc_counts])
    # tf-idf
    tfidf_matrix = tf_matrix_normalized * idf_vector
    tfidf_score = np.sum(tfidf_matrix, axis=1).reshape(-1, 1)
    # term weights
    neg_word_counts = np.sum(tf_matrix_np, axis=1).reshape(-1, 1) 
    term_weights = np.zeros_like(neg_word_counts)
    for i in range(len(doc_length_matrix)):
        if doc_length_matrix_np[i, 0] > 0:
            term_weights[i, 0] = neg_word_counts[i, 0] / doc_length_matrix_np[i, 0]
    return tfidf_score, term_weights


# def get_data(doc):

#     vdictionary = {}
#     _odata = [0] * 18 # Modified for CIK
#     total_syllables = 0
#     word_length = 0

#     tokens = re.findall('\w+', doc)  # Note that \w+ splits hyphenated words
#     for token in tokens:
        
#         if not token.isdigit() and len(token) > 1 and token in lm_dictionary:
#             _odata[2] += 1  # word count
#             word_length += len(token)
#             if token not in vdictionary:
#                 vdictionary[token] = 1
#             if lm_dictionary[token].positive: _odata[3] += 1
#             if lm_dictionary[token].negative: _odata[4] += 1
#             if lm_dictionary[token].uncertainty: _odata[5] += 1
#             if lm_dictionary[token].litigious: _odata[6] += 1
#             if lm_dictionary[token].weak_modal: _odata[7] += 1
#             if lm_dictionary[token].moderate_modal: _odata[8] += 1
#             if lm_dictionary[token].strong_modal: _odata[9] += 1
#             if lm_dictionary[token].constraining: _odata[10] += 1
#             total_syllables += lm_dictionary[token].syllables

#     _odata[11] = len(re.findall('[A-Z]', doc))
#     _odata[12] = len(re.findall('[0-9]', doc))
#     # drop punctuation within numbers for number count
#     doc = re.sub('(?!=[0-9])(\.|,)(?=[0-9])', '', doc)
#     doc = doc.translate(str.maketrans(string.punctuation, " " * len(string.punctuation)))
#     _odata[13] = len(re.findall(r'\b[-+\(]?[$€£]?[-+(]?\d+\)?\b', doc))
#     _odata[14] = total_syllables / _odata[2]
#     _odata[15] = word_length / _odata[2]
#     _odata[16] = len(vdictionary)

#     # Convert counts to %
#     for i in range(3, 10 + 1):
#         _odata[i] = (_odata[i] / _odata[2]) * 100
#     # Vocabulary

#     return _odata

def main():
    tfidf_score, term_weights = process()
    print(np.shape(tfidf_score))
    print(np.shape(term_weights))
    np.savetxt(f"./result/{EXP_SETTING}/tfidf_score.csv", tfidf_score, delimiter=",", fmt="%.6f")
    np.savetxt(f"./result/{EXP_SETTING}/term_weights.csv", term_weights, delimiter=",", fmt="%.6f")
        

if __name__ == '__main__':
    print('\n' + time.strftime('%c') + '\nGeneric_Parser.py\n')
    main()
    # filename = r"E:\NLP_Project1\Archive\data\2020\QTR1\20200331_10-Q_edgar_data_940944_0000940944-20-000014_1.txt"
    # fname = os.path.basename(filename)
    # cik = extract_cik_from_filename(fname)
    # date = extract_date_from_filename(fname)
    # print(cik)
    # print(date)
    print('\n' + time.strftime('%c') + '\nNormal termination.')