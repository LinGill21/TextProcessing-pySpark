# TextProcessing-pySpark
# By Lindsay Gillespie
## About
I am going to answer the question are old mysteries more plot-focused than a newer mystery novel. How I going to go about doing this is to scan the book The Mysterious Affair 
at Styles into Pyspark, then with pySpark I will clean the data, process the data and finally, I will graph the most commonly used words. If the detective name is the most 
commonly used word then the book is lead detective-driven. If some other word is most commonly used then the book is plot-driven.

## Data
For this project, I will use a book from [Project Gutenberg] (https://www.gutenberg.org/). The book I will use is The Mysterious Affair at Styles by Agatha Christie.
Once I find the book I will use the Plain Text version of the book. The thing about Gutenburg is they have license information at the bottom of each book. To keep that from
impacting the output of my text processing I copied and pasted the book into a notepad and removed the license information along with any other information Gutenburg added to 
the book. Once that is done I uploaded the book to Github that way I will have a URL to the book. In Github how to get the URL is to click the book in the repo then click raw 
or blame. The address in the address bar is the URL that will be used in the project.

## Databrick
For this project, I will be creating and running the code on a website called Databrick Community.
All you need to get Databrick running is to create an account. Once you have an account you go under the cluster tab and then click create a cluster.
Once a cluster is created go back to the main menu and click create a new notebook. Once for the notebook setting the language is python and for the cluster, you want to 
select the cluster you just created. Now the project is ready for code. 

## Pulling Data
In the notebook, I will pull the data into the notebook by using the library urllib.request. Then once the book is pulled in I will store the book at /tmp/ and call it 
Christie.txt
``` python
import urllib.request
urllib.request.urlretrieve("https://raw.githubusercontent.com/LinGill21/TextProcessing-pySpark/main/TheMysteriousAffairatStyles.txt" , "/tmp/Christie.txt")
```
Now to save the book. The method takes two arguments the first argument is where the book is now and the second argument is where you want the book to go. The first argument 
needs to start with file: and then the location. The second argument needs to start with dbfs: and then the location of where you want to store the file. I store my file in 
the data folder.
```python
dbutils.fs.mv("file:/tmp/Christie.txt","dbfs:/data/Christie.txt")
```
The final step is transferring the file into spark. Spark holds files in RDDs or Resilient Distributed Datasets. So we will be transforming our data into an RDD.
In Databrick spark is shortened to sc. Make sure to use the new file location when entering the location.
```
christieRDD = sc.textFile("dbfs:/data/Christie.txt")
```

## Cleaning the Data
The book is currently in book form with capitalization, punctuation, sentences, and stopwords. Stop words are just words that just make a sentence read better but don't 
add anything to the sentence. For example "the".  So to get the word count the first step is to flatmap and get rid of capitalization and spaces. Flatmapping is just breaking up the sentences into words.
```
wordsRDD=christieRDD.flatMap(lambda line : line.lower().strip().split(" "))
```
The second step is to remove all the punctuation. This will be achieved by using a regular expression that looks for anything that is not a letter. To use a regular expression we will need the library re.
```
import re
cleanTokensRDD = wordsRDD.map(lambda w: re.sub(r'[^a-zA-Z]','',w))
```
Now that the words are truly words we have to remove the stop words. PySpark already knows what words are stop words so all we need to do is to import the library StopWordsRemover from pyspark. Then filter out those words from the library.
```
from pyspark.ml.feature import StopWordsRemover
remover =StopWordsRemover()
stopwords = remover.getStopWords()
cleanwordRDD=cleanTokensRDD.filter(lambda w: w not in stopwords)
```

## Processing data
For processing data, all we have to do is change the words to the form (word,1) then count how many times we see the word and change the 2nd parameter to that count.
First step:
Map words to key-value pairs.
```
IKVPairsRDD= cleanwordRDD.map(lambda word: (word,1))
```
Second step:
Reduce by key. In our case, the key is the word so in other words we will have only one instance of a word in our list and count how many times it appeared before we reduced.
```
wordCountRDD = IKVPairsRDD.reduceByKey(lambda acc, value: acc+value)
```
Third step:
Return to python. Pyspark is nice but it is limited to what it can do compared to python. So we will return to python to do more complicated processing. This is done with the 
collect() function.
```
results = wordCountRDD.collect()
```

## Finding Useful Data
We just did a word count. A useful piece of data is the most common words in a story. To find the most common words we need to sort our list by the 2nd value. I found how to 
sort [here](https://www.kite.com/python/answers/how-to-sort-a-list-of-tuples-by-the-second-value-in-python). That function will sort in low to high so if we reverse the list 
we will have the most used words. I used list splicing to just show the top 20 words.
```python
results.sort(key=lambda x:x[1])
results.reverse()
print(results[:20])
```
Viewing a list of words is fine but it is better to chart data. To create a chart we will use the library mathplotlib. Here is a helpful stack overflow on how to graph a list 
of tuple using one side of the x axis and the other side for y axis.[https://stackoverflow.com/questions/13925251/python-bar-plot-from-list-of-tuples] (https://stackoverflow.com/questions/13925251/python-bar-plot-from-list-of-tuples)
```
mostCommon=results[1:14]
word,count = zip(*mostCommon)
import matplotlib.pyplot as plt
fig = plt.figure()
plt.barh(word,count)
plt.xlabel("Number of times used")
plt.ylabel("Most used words")
plt.title("Most used words in Myserious affair at styles")
plt.show()
```
Graph:

![GraphImg](https://github.com/LinGill21/TextProcessing-pySpark/blob/main/words.png)

## Conclusion
In the about it was stated that if the detective's name showed up in the top 20 the book would be classified as a character-driven book. The detective's name Poirot was the 
most used word in the book but in second was the last name of the victim. The victim's husband shares her last name and is a suspect in the case. The 8th most used word is 
john which is the name of the murderer. The 9th word is John's last name shared with his wife Emily who is a suspect. I believe due to the common use of other names besides 
poriet the novel is plot-driven not lead detective driven.

