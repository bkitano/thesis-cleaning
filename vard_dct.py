from gensim.corpora import Dictionary
from gensim.parsing.preprocessing import remove_stopwords
from gensim.parsing.preprocessing import strip_numeric
from gensim.parsing.preprocessing import strip_multiple_whitespaces
from gensim.parsing.preprocessing import remove_stopwords
from gensim.parsing.preprocessing import preprocess_string
from gensim.parsing.preprocessing import split_alphanum
from gensim.parsing.preprocessing import strip_punctuation
from gensim.parsing.preprocessing import strip_short
    
from os import listdir
import string
import time
import pickle 

from nltk.stem import WordNetLemmatizer
lemmatizer = WordNetLemmatizer() 

filePath = './vard_txt/'
embPath = './emb_tk/'
ldaPath = './lda_tk/'
files = listdir(filePath)
dct = Dictionary()

batchSize = 100
fileCount = len(files)
batches = [files[i:i + batchSize] for i in range(0, fileCount, batchSize)]
timeElapsed = 0

lastTime = time.time()
for batchIndex in range(1, len(batches)+1):
    
    batch = batches[batchIndex-1]
    
    for filename in batch:
        name = '.'.join(filename.split('.')[:-1])

        with open(filePath + filename, 'r') as fp:
            txt = fp.read()
        
            txt = strip_short(txt, minsize=2)

        CUSTOM_FILTERS = [lambda x: x.lower(), 
                              remove_stopwords, 
                              split_alphanum,
                              strip_punctuation,
                              strip_multiple_whitespaces,
                              strip_numeric
                             ]

        tokens = preprocess_string(txt, CUSTOM_FILTERS)

        # emb
        with open(embPath + name + '.p', 'wb') as fp:
            pickle.dump(tokens, fp)

        # lda tokens should be stemmed
        ldaTokens = [lemmatizer.lemmatize(token) for token in tokens]
        with open(ldaPath + name + '.p', 'wb') as fp:
            pickle.dump(ldaTokens, fp)

        dct.add_documents([tokens])

    batchTime = time.time() - lastTime
    timeElapsed += batchTime
    ETA = (timeElapsed/batchIndex) * (len(batches) - batchIndex)
    ETAstring = "{}:{}:{}".format( int(ETA / 3600), int( (ETA % 3600) / 60 ), int(ETA % 60))

    print("Batch {} of {} | dct: {} words | Batch time: {:.4} | ETA: {}".format(batchIndex, len(batches), len(dct), batchTime, ETAstring))
    lastTime = time.time()

dct.filter_extremes(no_below=5, keep_n=25000)
dct.save('./vard.dct')