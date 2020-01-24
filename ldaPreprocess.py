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
import re
from multiprocessing import Pool

from gensim.corpora import Dictionary
from nltk.stem import WordNetLemmatizer
from nltk import pos_tag
from nltk.corpus import wordnet

lemmatizer = WordNetLemmatizer() 


# correction OCR mistake dictionary
CORRECTION = './rules/CorrectionRules.txt'
with codecs.open(CORRECTION, 'r', encoding='utf8') as f:
    lines = f.read().split('\n')
    pairs = ([(line.split()[0], line.split()[1])
              for line in lines if line != ''])
    correctionDict = dict(pairs)

# create syncopate dictionary
SYNCOPATE = './rules/SyncopeRules.txt'
with codecs.open(SYNCOPATE, 'r', encoding='utf8') as f:
    lines = f.read().split('\n')
    pairs = ([(line.split()[0], line.split()[1])
              for line in lines if line != ''])
    syncopateDict = dict(pairs)

# create variants dictionary
VARIANT = './rules/VariantSpellings.txt'
with codecs.open(VARIANT, 'r', encoding='utf8') as f:
    lines = f.read().split('\n')
    pairs = ([(line.split()[0], line.split()[1])
              for line in lines if line != ''])
    variantDict = dict(pairs)

# other variants dict
VARIANT_2 = './rules/variants.txt'
with codecs.open(VARIANT_2, 'r', encoding='utf8') as f:
    lines = f.read().split('\n')
    pairs = [(lines[i], lines[i+1].split('\t')[1])
             for i in range(0, len(lines)-1, 2)]
    variantDict2 = dict(pairs)

ROMAN_NUMERALS = './rules/romannumerals.txt'
with open(ROMAN_NUMERALS, 'r') as f:
    romanNumeralList = f.read().split('\n')
    
# ------- HELPERS -------

"""Map POS tag to first character lemmatize() accepts"""
def get_wordnet_pos(word):
    tag = pos_tag([word])[0][1][0].upper()
    tag_dict = {"J": wordnet.ADJ,
                "N": wordnet.NOUN,
                "V": wordnet.VERB,
                "R": wordnet.ADV}

    return tag_dict.get(tag, wordnet.NOUN) # if you can't figure it out, it's a noun

def ncleaner(path, d):
    with codecs.open(path, encoding='utf8') as f:
        text = f.read().split('\n')[0]
        text1 = unidecode.unidecode(text)
        CUSTOM_FILTERS = [lambda x: x.lower(), 
                          remove_stopwords, 
                          strip_numeric, 
                          strip_multiple_whitespaces, 
                          strip_punctuation]
        a = preprocess_string(text1, CUSTOM_FILTERS)
        b = [lemmatizer.lemmatize(word, get_wordnet_pos(word)) for word in a]
        tokens = parallelRemove(b, 4)
        return tokens
    
def removeRomanNumerals(w):
    count = 0
    for n in romanNumeralList:
        matcher = r'\W({})[\s\.]?\W'.format(n)
        w = re.sub(matcher, ' ', w)
    return w

    
def replaceWordsFromMap(tokens, correctionsMap):
    for i in range(len(tokens)):
        word = tokens[i]
        if correctionsMap.get(word) is not None:
            replacement = correctionsMap[word]
            tokens[i] = replacement

    return tokens


def cleaner(path, d):
    with codecs.open(path, encoding='utf8') as f:
        text = f.read()
        text1 = unidecode.unidecode(text)

        # remove roman numerals
        text1 = removeRomanNumerals(text1)
        
        # strip numbers, whitespace, and punctuation
        EMBEDDING_FILTERS = [lambda x: x.lower(), 
                             strip_numeric,
                             strip_multiple_whitespaces,
                             strip_punctuation]
        c = preprocess_string(text1, EMBEDDING_FILTERS)
        
        # replace and correct words
        c = replaceWordsFromMap(c, correctionDict)
        c = replaceWordsFromMap(c, syncopateDict)
        c = replaceWordsFromMap(c, variantDict)
        c = replaceWordsFromMap(c, variantDict2)

        d = [lemmatizer.lemmatize(word, get_wordnet_pos(word)) for word in c]
        
        t = " ".join(d)
        
        tokens = parallelRemove(d, 4)
        
        return tokens, t

# ------- SET UP --------
filePath = './vep-txt/'
files = listdir(filePath)

rwfd2 = pickle.load(open('../dtm/rwfd2.p','rb'))
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

tokensPath = './vep-tokens/'
textPath = './vep-cleanedText/'

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
        
        tokens, t = cleaner(path, rwfd2)
        dct.add_documents([tokens])
        
        with open(tokensPath + name + '.p', 'wb') as fp:
            pickle.dump(tokens, fp)
            
        with open(textPath + name + '.txt', 'w') as fp:
            fp.write(t)

    batchTime = time.time() - lastTime
    timeElapsed += batchTime
    ETA = (timeElapsed/batchIndex) * (len(batches) - batchIndex)
    ETAstring = "{}:{}:{}".format( int(ETA / 3600), int( (ETA % 3600) / 60 ), int(ETA % 60))

    print("Batch {} of {} | dct: {} words | Batch time: {:.4} | ETA: {}".format(batchIndex, len(batches), len(dct), batchTime, ETAstring))
    lastTime = time.time()

dct.save('./vep1.dct')