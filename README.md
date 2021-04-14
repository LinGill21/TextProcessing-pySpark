# TextProcessing-pySpark
## About
WRITE THIS!!!!!!!!!!!!

## Data
For this project, I will use a book from [Project Gutenberg] (https://www.gutenberg.org/). The book I will use is The Mysterious Affair at Styles by Agatha Christie.
Once I find the book I will use the Plain Text version of the book. The thing about Gutenburg is they have license information at the bottom of each book. To keep that from
impacting the output of my text processing I copied and pasted the book into a notepad and removed the license information along with any other information Gutenburg added to 
the book. Once that is done I uploaded the book to Github that way I will have a URL to the book. In Github how to get the URL is to click the book in the repo then click raw or 
blame. The address in the address bar is the URL that will be used in the project.

## Databrick
For this project, I will be creating and running the code on a website called Databrick Community.
All you need to get Databrick running is to create an account. Once you have an account you go under the cluster tab and then click create a cluster.
Once a cluster is created go back to the main menu and click create a new notebook. Once for the notebook setting the language is python and for the cluster, you want to select 
the cluster you just created. Now the project is ready for code. 

## Pulling Data
In the notebook, I will pull the data into the notebook by using the library urllib.request. Then once the book is pulled in I will store the book at /tmp/ and call it Christie.txt
``` python
import urllib.request
urllib.request.urlretrieve("https://raw.githubusercontent.com/LinGill21/TextProcessing-pySpark/main/TheMysteriousAffairatStyles.txt" , "/tmp/Christie.txt")
```
Now to save the book. The method takes two arguments the first argument is where the book is now and the second argument is where you want the book to go. The first argument needs to start with file: and then the location. The second argument needs to start with dbfs: and then the location of where you want to store the file. I store my file in the data folder.
```python
dbutils.fs.mv("file:/tmp/Christie.txt","dbfs:/data/Christie.txt")
```
The final step is transferring the file into spark. Spark holds files in RDDs or Resilient Distributed Datasets. So we will be transforming our data into an RDD.
In Databrick spark is shortened to sc. Make sure to use the new file location when entering the location.
```
christieRDD = sc.textFile("dbfs:/data/Christie.txt")
```


