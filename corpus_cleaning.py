# coding: utf-8

# First, we'll read in a piece of example text.

import codecs
import json
import re
import pandas as pd
# import unidecode

# correction OCR mistake dictionary
CORRECTION = '/Users/bkitano/Desktop/Classes/Spring_2019/thesis/corpus/rules/CorrectionRules.txt'
with codecs.open(CORRECTION, 'r', encoding='utf8') as f:
    lines = f.read().split('\n')
    pairs = ([(line.split()[0], line.split()[1])
              for line in lines if line != ''])
    correctionDict = dict(pairs)

# create syncopate dictionary
SYNCOPATE = '/Users/bkitano/Desktop/Classes/Spring_2019/thesis/corpus/rules/SyncopeRules.txt'
with codecs.open(SYNCOPATE, 'r', encoding='utf8') as f:
    lines = f.read().split('\n')
    pairs = ([(line.split()[0], line.split()[1])
              for line in lines if line != ''])
    syncopateDict = dict(pairs)

# create variants dictionary
VARIANT = '/Users/bkitano/Desktop/Classes/Spring_2019/thesis/corpus/rules/VariantSpellings.txt'
with codecs.open(VARIANT, 'r', encoding='utf8') as f:
    lines = f.read().split('\n')
    pairs = ([(line.split()[0], line.split()[1])
              for line in lines if line != ''])
    variantDict = dict(pairs)

# other variants dict
VARIANT_2 = '/Users/bkitano/Desktop/Classes/Spring_2019/thesis/corpus/rules/variants.txt'
with codecs.open(VARIANT_2, 'r', encoding='utf8') as f:
    lines = f.read().split('\n')
    pairs = [(lines[i], lines[i+1].split('\t')[1])
             for i in range(0, len(lines)-1, 2)]
    variantDict2 = dict(pairs)

ROMAN_NUMERALS = '/Users/bkitano/Desktop/Classes/Spring_2019/thesis/corpus/rules/romannumerals.txt'
with open(ROMAN_NUMERALS, 'r') as f:
    romanNumeralList = f.read().split('\n')

# name to date dictionary
catalogPath = '/Users/bkitano/Desktop/Classes/Spring_2019/thesis/corpus/eebo-tcp/tcp-texts/TCP.csv'
df = pd.read_csv(catalogPath, index_col='TCP')
nameToDateDict = df[['Date']].to_dict()['Date']

import pickle
with open('./nameToDate.p', 'wb') as fp:
    pickle.dump(nameToDateDict, fp)

    # remove extraneous whitespace
def removeWhitespace(w, editsString):
    (r, count) = re.subn(r'\s+', ' ', w, count=0)
    if count:
        editsString += 'w#{}'.format(count)
    if r != '':
        return r, editsString

# remove unnecessary punctuation
def removeUnnecessaryPunctuation(w, editsString):
    (r, count) = re.subn(
        r'[!\"#\$%&\(\)\*\+\,\/:;<=>\?@\[\\\]\^_`\{\|\}~]', '', w, count=0)
    if count:
        editsString += 'rUP#{}'.format(count)
    if r != '':
        return r, editsString

# also want to remove the s. or mr., so replace \s\w{1}\. with \w
def removeAbbreviatedNames(w, editsString):
    r = re.sub(r'\s([\w\d]){1}\.', '', w)
    if r != '':
        return r


def removeRomanNumerals(w, editsString):
    count = 0
    for n in romanNumeralList:
        matcher = r'\W({})[\s\.]?\W'.format(n)
        (w, i) = re.subn(matcher, ' ', w, count=count)
        count += i
    if count:
        editsString += 'rRN#{}'.format(count)
    return w, editsString


# compiling a list of EEBO symbols to remove
symbolsDict = {
    u'\u3008\u25ca\u3009': ' ',  # the diamond divider
    u'\u3008\u2026\u3009': ' ',  # ellipses
    u'\u2022': '', # one big black dot
    
}


def replaceSymbolsFromList(w, symbolDict, editsString):
    count = 0
    for symbol in symbolDict.keys():
        matcher = u'\s*{}\s*'.format(symbol)
        (w, i) = re.subn(matcher, symbolDict[symbol], w, count=0)
        count += i
    if count:
        editsString += 'rSFL#{}'.format(count)
    return w, editsString

"""
sentences - list of tokenized sentences
correctionsMap - dictionary of corrections {to replace : replacement}
editsString - existing editString
prefix - prefix to write into the editString
replacementDict - store all the actual corrections made
"""

def replaceWordsFromMap(sentences, correctionsMap, editsString, prefix, replacementDict):
    cTally = 0
    for sentence in sentences:
        for i in range(len(sentence)):
            word = sentence[i]
            try:
                replacement = correctionsMap[word]
                sentence[i] = replacement
                replacementDict[word] = replacement
                cTally += 1
            except KeyError:
                pass

    editsString += "{}R#{}".format(prefix, cTally)
    return (sentences, replacementDict, editsString)

# get year from name of file
def getYearFromDocID(docID):
    try:
        year = nameToDateDict[docID]
        firstYear = year.split('-')[0]
        return firstYear
    except:
        return None

# writing to a txt file
def writeToFile(sentenceList, docID, editsString, docReplacements, path):
    year = getYearFromDocID(docID)

    # turn list of words into sentence
    # write sentence to file
    cleanedFileName = path + year + '-' + docID + '.txt'

    with codecs.open(cleanedFileName, 'a+', encoding='utf8') as f:
        for sentence in sentenceList:
            s = ' '.join(sentence) + '. '
            f.write(s)
        f.write('\n\n' + editsString + '\n\n')
        f.write(json.dumps(docReplacements))

# --------- CONTAINER ---------
def container(textpath, filename, cleanedPathName):
    name = '.'.join(filename.split('.')[:-1])
    
    with codecs.open(textpath + filename, encoding='utf8') as f:
        text = f.read()

    # lower case everything
    editString = ""
    docReplacements = dict()

    text1 = unidecode.unidecode(text)
    text1 = text1.lower()
    editString += 'l'
    (text1, editString) = removeWhitespace(text1, editString)
    (text1, editString) = removeUnnecessaryPunctuation(text1, editString)
    (text1, editString) = removeRomanNumerals(text1, editString)
    (text1, editString) = removeAbbreviatedNames(text1, editString)
    (text1, editString) = replaceSymbolsFromList(text1, symbolsDict, editString)

    sentences = text1.split('.')
    tokenizedSentences = [sentence.strip().split(' ') for sentence in sentences]
    # originalSentences = list(filter(lambda s: len(s) >= 4, tokenizedSentences))
    longSentences = list(filter(lambda s: len(s) >= 4, tokenizedSentences))

    # weirdChars = [u'\xf3']

    (longSentences, docReplacements, editString) = replaceWordsFromMap(
        longSentences, correctionDict, editString, 'c', docReplacements)

    (longSentences, docReplacements, editString) = replaceWordsFromMap(
        longSentences, syncopateDict, editString, 's', docReplacements)

    (longSentences, docReplacements, editString) = replaceWordsFromMap(
        longSentences, variantDict, editString, 'v', docReplacements)

    (longSentences, docReplacements, editString) = replaceWordsFromMap(
        longSentences, variantDict2, editString, 'v2', docReplacements)

    writeToFile(longSentences, name, editString,
                docReplacements, cleanedPathName)

# ----------- EXECUTOR ------------
from os import listdir
from os.path import isfile, join
import time 

fromPath = '/Users/bkitano/Desktop/Classes/Spring_2019/thesis/corpus/eebo-tcp/tcp-txt/'
toPath = '/Users/bkitano/Desktop/Classes/Spring_2019/thesis/corpus/cleaned_txt/'
# onlyfiles = [f for f in listdir(fromPath) if isfile(join(fromPath, f))]

startTime = time.time()

print("Starting cleaning")

batchCount = 0
running_batch_time = 0
interval = 20
batchesTotal = len(onlyfiles) / interval
last_time = time.time()

for i in range(len(onlyfiles)):

    if(i % interval == (interval - 1)):
        batchCount += 1
        batchTime = time.time() - last_time
        running_batch_time += batchTime
        avg_batch_time = running_batch_time / batchCount
        ETA = (len(onlyfiles) - i) * avg_batch_time / interval
        ETAstring = "{}:{}:{}".format( int(ETA / 3600), int( (ETA % 3600) / 60 ), int(ETA % 60))
        print( "{} of {} | batch time: {} | ETA: {}".format(batchCount, batchesTotal, batchTime, ETAstring))
        last_time = time.time()
    try:
        container(fromPath, onlyfiles[i], toPath)
    except:
        print(onlyfiles[i])

elapsedTime = time.time() - startTime
print("total time: {} seconds".format(elapsedTime))

# --------- GO OVER -----------
failedDocs = [
    'A57504.txt', 'A86214.txt', 'K005913.000.txt', 'K110277.000.txt', 'A50324.txt', 'N04647.txt',
    'N18443.txt', 'A52368.txt', 
    'A71352.txt',
    'A36186.txt', 
    'A05601.txt',
    'A62314.txt',
    'A04052.txt',
    'A30525.txt',
    'A07661.txt',
    'A32624.txt',
    'A16974.txt',
    'N04376.txt',
    'A01831.txt',
    'A49534.txt',
    'A37835.txt',
    'A42476.txt',
    'A13822.txt'
]

failedUnicodeChars = [
    u'\ud83d',
    u'\udf15',
    u'\ud834',
    u'\udd22',
    u'\udd21',
    u'\udd1e',
    u'\uddb9',u'\uddc4',u'\uddc5',u'\uddc6', u'\uddc3', u'\uddc2',u'\uddc1',u'\udd00', u'\udd01',u'\udd77',
        u'\udd15', u'\udd14', u'\udd10',u'\udd34', u'\udd5d', u'\udf0d', u'\udf14',u'\uddb6', u'\uddb7',
            u'\uddb8',u'\uddcb',u'\uddcd',u'\uddc8',u'\udf04',u'\udf39',u'\udf5e', u'\udf5f', u'\udf02',
                u'\uddc7', u'\uddca', u'\uddcc',u'\uddce' ,u'\udf46',u'\udf0a',u'\udf0b', u'\udf05' ,u'\udf06',
                    u'\udf6a',u'\udf13',u'\udf2d',u'\udf4a', u'\udf57',u'\udf58',u'\udf63',u'\udf18',u'\udf3f',
     u'\udf16', u'\udf55',u'\udf01',u'\udf03',u'\udf6f',u'\udf3a',u'\udd06',u'\udd11',u'\udf41',u'\ud800',u'\udd96',
]
"""