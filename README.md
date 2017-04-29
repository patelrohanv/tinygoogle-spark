# tinygoogle-spark
Repository for Project 2 of CS 1699: Special Topics in Computer Science - Cloud Computing

## Project 2 - Spark

This is an implementation of a tinyGoogle search engine that readings in text files and allows a user to search for term(s) 
and get an output with how many times that term appears in each text file and how that number is weighted relative to the
other documents. 

### Implementation
 
We implemented this project using Apache Spark, specifically PySpark. Using spark we were able to run multiple maps and reduces in less than 10 lines of code to generate the data that we stored in our inverted index. We also added the functionally to store the inverted index as a JSON to that users could load in an existing inverted index instead of generating a new one each time the program is run.
