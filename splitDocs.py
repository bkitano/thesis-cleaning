from os import listdir
import argparse

parser = argparse.ArgumentParser(description='Split documents in a given directory into given lengths.')
parser.add_argument('inputDir', type=str, help='Input directory for documents to be split.')
parser.add_argument('outputDir', type=str, help='Output directory for split documents to be written to.')
parser.add_argument('size', type=int, help='Length of each split document.')

args = parser.parse_args()

filePath = args.inputDir
files = listdir(filePath)

splitDocsPath = args.outputDir

for fileName in files:
    with open(filePath + fileName, 'r+') as fp:
        text = fp.read()
        words = text.split(" ")
        
    splits = [i for i in range(0, len(words), args.size)]
    splits.append(len(words))
    
    frags = [words[splits[i]:splits[i+1]] for i in range(len(splits) - 1)]
    
    for i, frag in zip(range(len(frags)), frags):
        fragText = " ".join(frag)
        fragTitle = fileName.split('.txt')[0] + '-' + str(i) + '.txt'
        
        with open(splitDocsPath + fragTitle, 'w+') as _fp:
            _fp.write(fragText)