#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import print_function

import os
import sys
from operator import add

import string
from string import digits
from string import punctuation

from pyspark import SparkContext

if __name__ == "__main__":
    if len(sys.argv) != 2:
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
    for (word, name, count) in output:
        word = word.lower()
        i = name.find("/books/") + 7
        j = name.find(".txt")
        name = name[i:j]
        print("%s -> %s: %i" % (word.encode("utf-8"), name.encode("utf-8"), count))

    sc.stop()
