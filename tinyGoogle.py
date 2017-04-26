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
        if term == ':q' or term == ':Q':
            quit = True
            break
    if not quit:
        print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        print("\tPlease wait while the search is completed ... ")
        print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        for term in terms:                                                            #loop through keywords
            term = cleanWord(term)
            if term not in Results:                                                          #Only calculate if not already done
                if term in ii:                                                                 # Only calculate if keyword in any file
                    Results[term] = {}                                                               #Create Results dict
                    for fileName in fileList:                                                           #Loop through files
                        freq = 0                                                                            #initializ freq to zero
                        try:                                                                                #handle exceptions where keyword isnt in file
                            freq = ii[term][fileName]                                                      #calculate frequency of keyword in file
                        except KeyError:                                                                    # handle exceptions where keyword isnt in file
                            Results[term][fileName] = [0.000000, 0.000000]                                           # handle exceptions where keyword isnt in file
                        if (freq > 0):                                                                      #calculate TF
                            tf = 1 + math.log(freq, 2)                                                          # as 1+log2(freq) or 0
                        else:                                                                                   # depending on
                            tf = 0                                                                              # Frequency

                        IDF = math.log((float(numFiles) / len(ii[term])), 2)                           #calculate IDF = log2(N/n)
                        weight = tf * IDF                                                                   #calculate weight = TF * IDF
                        Results[term][fileName] = [weight, freq]                                                 #add weight for keyword, file
                else:                                                                               #Handle cases where
                    for fileName in fileList:                                                           # keyword isnt in
                        if term not in Results:                                                          # any file
                            Results[term] = {fileName : [0.000000, 0.000000]}                                             # if first time
                        else:                                                                               #
                            Results[term][fileName] = [0.000000,0.000000]                                               # if already in results for some file

        for searchTerm in Results.keys():
            termResults = Results[searchTerm]
            sortedTermResults = termResults
            #sortedTermResults = sorted(termResults, key=lambda x: termResults.values()[1][0])
            #print (sortedTermResults)
            print("\n\t\tThe term \""+searchTerm+"\" is found in:\n")
            print("--------------------------------------------------------------------")
            for doc in sortedTermResults.keys():
                print ("Document: "+ re.sub(r"(?<=\w)([A-Z]|by|of|and)", r" \1", doc))
                print ("Weight:\t\t"+str(sortedTermResults[doc][0]))
                print ("Frequency:\t"+str(sortedTermResults[doc][1]))
                print("--------------------------------------------------------------------")
            print("____________________________________________________________________")

if __name__ == "__main__":
    print("____________________________________________________________________");
    print("\t\tWelcome to tiny-Google");
    print("\tBy Salvatore Avena and Rohan Patel");
    print("____________________________________________________________________");
    indexFile = './inverted-index.json'

    ii = {}
    fileList =[]
    build = True

    if os.path.isfile(indexFile):
        while True:
            indexResponse = raw_input("An index already exists on disk.\nWould you like to use the existing index or create a new one?\n\t1. Use the existing index\n\t2. Create a new one\n")
            if int(indexResponse) == 1:
                print("Ok, existing index will be used.")
                print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
                print("\tPlease wait ... ");
                print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
                build = False
                break
            elif int(indexResponse) == 2:
                print("Ok, existing index will be deleted.")
                print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
                print("\tPlease wait ... ");
                print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
                build = True
                break
            else:
                print("Not a valid option. Please try again.\n")
        print("____________________________________________________________________")

    if build:
        print("____________________________________________________________________")
        print("Creating New Index.");
        print("____________________________________________________________________")
        print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        print("\tPlease wait while the index is generated ... ");
        print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        os.system('spark-submit file:///mounted_volume/indexer.py file:///mounted_volume/books/*.txt')
        print("____________________________________________________________________")

    with open(indexFile, "r+") as f:
        ii = json.load(f)

    fileList = ii['fileList']
    numFiles = len(fileList)

    while not quit:
        Results = {}
        searchTerms = raw_input("Enter a search query (or ':q' to quit): ")
        terms = searchTerms.split()
        search(terms)

print("____________________________________________________________________")
print("Goodbye!")
print("____________________________________________________________________")
