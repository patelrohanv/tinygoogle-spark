val f = sc.textFile(inputPath)
val w = f.flatMap(l => l.split(“ “)).map(word=> (word, 1)).reduceByKey(_+_)
w.saveAsTextFile(outputPath)
