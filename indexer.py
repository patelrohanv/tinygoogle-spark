from __future__ import print_function

import re
import sys
import json
import string

from string import digits
from string import maketrans
from string import punctuation
from pyspark import SparkContext

def cleanFile (name):
    i = name.find("/books/") + 7
    j = name.find(".txt")
    name = name[i:j]
    return name

def cleanWord (word):
    word = re.sub('[^a-zA-Z]+', '', word.lower())
    return word

if __name__ == "__main__":
    sc = SparkContext(appName="PythonWordCount")
    line = sc.textFile("file:///mounted_volume/books/*.txt")
    lines = sc.textFile(sys.argv[1], 1)

    counts = sc.wholeTextFiles(sys.argv[1], 1)\
        .flatMap(lambda (name, content): map(lambda word: (word, name), content.split(' ')))\
        .map(lambda (word, name): ((cleanWord(word), cleanFile (name)), 1))\
        .reduceByKey(lambda count1, count2: count1 + count2)\
        .map(lambda ((word, name), count): (word, name, count))
    output = counts.collect()

    ii = {}
    fileList =[]
    indexFile = './inverted-index.json'

    for (word, name, count) in output:
        if word not in ii:
            ii[word] = {}
            ii[word][name] = count
        else:
            ii[word][name] = count
        if name not in fileList:
            fileList.append(name)

    ii['fileList']=fileList

    with open(indexFile, "w+") as g:                                                  #save dict
        json.dump(ii, g)

    sc.stop()
