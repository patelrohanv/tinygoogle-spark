from __future__ import print_function

import os
import sys
import math
import collections
from collections import OrderedDict
import re
from operator import add

import string
from string import digits
from string import punctuation
from string import maketrans

from pyspark import SparkContext

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: wordcount <file>", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName="PythonWordCount")
    line = sc.textFile("file:///mounted_volume/books/*.txt")
    lines = sc.textFile(sys.argv[1], 1)
    # counts = lines.flatMap(lambda x: x.split(' ')) \
    #               .map(lambda x: (x, 1)) \
    #               .reduceByKey(add)

    counts = sc.wholeTextFiles(sys.argv[1], 1)\
		.flatMap(lambda (name, content): map(lambda word: (word, name), content.split(' ')))\
		.map(lambda (word, name): ((word, name), 1))\
		.reduceByKey(lambda count1, count2: count1 + count2)\
        .map(lambda ((word, name), count): (word, name, count))

    output = counts.collect()
    ii = {}
    fileList =[]

    for (word, name, count) in output:
        word = word.lower().translate(None, string.punctuation).translate(None, "0123456789")
        i = name.find("/books/") + 7
        j = name.find(".txt")
        name = name[i:j]

        if word not in ii:
            #print("Added %s to ii with word %s and count %i" % (name.encode("utf-8"), word.encode("utf-8"), count))
            ii[word] = {}
            ii[word][name] = count
        else:
            #print("updated count for: %s to ii with word %s and to new count %i" % (name.encode("utf-8"), word.encode("utf-8"), count))
            ii[word][name] = count

        if name not in fileList:
            fileList.append(name)

    numFiles = len(fileList)
    Results = {}
    search = sys.argv[2]
    terms = search.split()

    for testWord in ii.keys():
        print('Word: '+testWord)
        for testDoc in ii[testWord].keys():
            print('\tDoc: '+testDoc)
            print('\t\tCount: '+(str)ii[testWord][testDoc])


    for term in terms:                                                            #loop through keywords
        if term not in Results:                                                          #Only calculate if not already done
            if term in ii:                                                                 # Only calculate if keyword in any file
                Results[term] = {}                                                               #Create Results dict
                for fileName in fileList:                                                           #Loop through files
                    freq = 0                                                                            #initializ freq to zero
                    try:                                                                                #handle exceptions where keyword isnt in file
                        freq = ii[term][fileName]                                                      #calculate frequency of keyword in file
                    except KeyError:                                                                    # handle exceptions where keyword isnt in file
                        Results[term][fileName] = 0.000000                                           # handle exceptions where keyword isnt in file
                    if (freq > 0):                                                                      #calculate TF
                        tf = 1 + math.log(freq, 2)                                                          # as 1+log2(freq) or 0
                    else:                                                                                   # depending on
                        tf = 0                                                                              # Frequency
                    IDF = math.log((float(numFiles) / len(ii[term])), 2)                           #calculate IDF = log2(N/n)
                    weight = tf * IDF                                                                   #calculate weight = TF * IDF
                    Results[term][fileName] = weight                                                 #add weight for keyword, file
            else:                                                                               #Handle cases where
                for fileName in fileList:                                                           # keyword isnt in
                    if term not in Results:                                                          # any file
                        Results[term] = {fileName : 0.000000}                                             # if first time
                    else:                                                                               #
                        Results[term][fileName] = 0.000000                                               # if already in results for some file

    for searchTerm in Results.keys():
        termResults = Results[term]
        sortedTermResults = sorted(termResults, key=lambda x: termResults.values())
        print (searchTerm + "_____________________________")
        for doc in termResults.keys():
            print ("----Document: "+ doc)
            print ("------------Weight: "+str(termResults[doc]))
    sc.stop()
