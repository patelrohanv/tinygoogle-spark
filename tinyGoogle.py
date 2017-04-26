from __future__ import print_function

import re
import os
import sys
import json
import math
import string
import os.path
import collections

from operator import add
from string import digits
from string import maketrans
from string import punctuation
from collections import OrderedDict

quit = False

def cleanWord (word):
    word = re.sub('[^a-zA-Z]+', '', word.lower())
    return word

def search (terms):
    global quit
    for term in terms:                                                            #loop through keywords
        if term == ':q':
            quit = True
            break
        term = cleanWord(term)
        if term not in Results:                                                          #Only calculate if not already done
            if term in ii:                                                                 # Only calculate if keyword in any file
                Results[term] = {}                                                               #Create Results dict
                for fileName in fileList:                                                           #Loop through files
                    freq = 0                                                                            #initializ freq to zero
                    try:                                                                                #handle exceptions where keyword isnt in file
                        freq = ii[term][fileName]                                                      #calculate frequency of keyword in file
                    except KeyError:                                                                    # handle exceptions where keyword isnt in file
                        Results[term][fileName] = 0.000000                                           # handle exceptions where keyword isnt in file
                        print("KEY ERROR, assigning weight=0")
                    if (freq > 0):                                                                      #calculate TF
                        tf = 1 + math.log(freq, 2)                                                          # as 1+log2(freq) or 0
                    else:                                                                                   # depending on
                        tf = 0                                                                              # Frequency

                    print("FREQ IS: "+(str)freq)
                    IDF = math.log((float(numFiles) / len(ii[term])), 2)                           #calculate IDF = log2(N/n)
                    print("IDF IS: "+(str)IDF)
                    weight = tf * IDF                                                                   #calculate weight = TF * IDF
                    print("WEIGHT IS: "+(str)weight)
                    Results[term][fileName] = weight                                                 #add weight for keyword, file
            else:                                                                               #Handle cases where
                print("____________IS NOT IN II")
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

if __name__ == "__main__":
    indexFile = './inverted-index.json'

    ii = {}
    fileList =[]
    if os.path.isfile(indexFile):
        print("Using existing index")
    else:
        print("ABOUT TO BUILD INDEX")
        os.system('spark-submit file:///mounted_volume/indexer.py file:///mounted_volume/books/*.txt')

    with open(indexFile, "r+") as f:
        ii = json.load(f)

    fileList = ii['fileList']
    numFiles = len(fileList)

    while not quit:
        Results = {}
        searchTerms = raw_input("Enter your search terms (or ':q' to quit): ")
        terms = searchTerms.split()
        search(terms)
