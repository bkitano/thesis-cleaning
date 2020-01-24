from gensim.parsing.preprocessing import preprocess_string
from gensim.parsing.preprocessing import strip_numeric
from gensim.parsing.preprocessing import strip_multiple_whitespaces
from gensim.parsing.preprocessing import strip_punctuation
from gensim.parsing.preprocessing import remove_stopwords
import time
import codecs
import unidecode
from os import listdir
from utils import parallelRemove
import pickle
from multiprocessing import Pool
import math

from gensim.corpora import Dictionary
from nltk.stem import WordNetLemmatizer

import argparse
parser = argparse.ArgumentParser(description='Tokenize a corpus.')
parser.add_argument('corpusDir', type=str, help='Corpus directory to digest')
parser.add_argument('tokensDir', type=str, help='Tokens directory to outout')
parser.add_argument('dictName', type=str, help='Name of dictionary output')
parser.add_argument('--keep_n', type=int, help='Keep n most frequent words in the dictionary')
parser.add_argument('--no_above', type=int, help='Keep words used in at least n documents')

args = parser.parse_args()

lemmatizer = WordNetLemmatizer() 

# ------- HELPERS -------

def cleaner(path, d):
    with codecs.open(path, encoding='utf8') as f:
        text = f.read().split('\n')[0]
        text1 = unidecode.unidecode(text)
        CUSTOM_FILTERS = [lambda x: x.lower(), 
                          remove_stopwords, 
                          strip_numeric, 
                          strip_multiple_whitespaces, 
                          strip_punctuation]
        a = preprocess_string(text1, CUSTOM_FILTERS)
        b = parallelRemove(a, 4)
        c = [lemmatizer.lemmatize(w) for w in b]
        return c

# ------- SET UP --------
filePath = args.corpusDir
files = listdir(filePath)

rwfd2 = pickle.load(open('../dtm/remove_words.p','rb'))
def parallelR(token):
    try:
        rwfd2[token]
        pass
    except:
        return token
    
def parallelRemove(tokens, p):
    with Pool(processes = p) as pool:
        results = pool.map(parallelR, tokens)
        pool.close()
        pool.join()
    return [r for r in results if r is not None]

gensimPath = args.tokensDir
dct = Dictionary()

# ------- MAIN ----------
                
batchSize = 100
fileCount = len(files)
batches = [files[i:i + batchSize] for i in range(0, fileCount, batchSize)]
timeElapsed = 0

lastTime = time.time()
for batchIndex in range(1, len(batches)+1):
    
    batch = batches[batchIndex-1]
    
    for filename in batch:
        path = filePath + filename
        name = '.'.join(filename.split('.')[:-1])
        
        tokens = cleaner(path, rwfd2)
        dct.add_documents([tokens])
        with open(gensimPath + name + '.p', 'wb') as fp:
            pickle.dump(tokens, fp)

    batchTime = time.time() - lastTime
    timeElapsed += batchTime
    ETA = (timeElapsed/batchIndex) * (len(batches) - batchIndex)
    ETAstring = "{}:{}:{}".format( int(ETA / 3600), int( (ETA % 3600) / 60 ), int(ETA % 60))

    print("Batch {} of {} | dct: {} words | Batch time: {:.4} | ETA: {}".format(batchIndex, len(batches), len(dct), batchTime, ETAstring))
    lastTime = time.time()

# we need to filter the extremes before we save the dictionary.
if args.keep_n:
    dct.filter_extremes(keep_n = args.keep_n)
elif args.no_above:
    dct.filter_extremes(no_above = args.no_above)
else:
    dct.filter_extremes(no_above = math.ceil( dct.num_docs * .001 ))
dct.save(args.dictName)