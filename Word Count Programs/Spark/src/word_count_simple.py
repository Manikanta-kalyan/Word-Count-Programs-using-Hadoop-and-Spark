# Import necessary libraries
import string

# Define function to count words and perform additional requirements
def countWords(sc, file, stopwords_file):
  # Load text file into RDD
  lines = sc.textFile(file)
  
  # Load stop words file into a Python list
  with open(stopwords_file) as f:
    stopwords = [word.strip() for word in f]
  #print(stopwords)
  # Split each line into words, convert to lower case, remove punctuation, and filter out stop words
  words = lines.flatMap(lambda x: x.split(' ')) \
              .map(lambda x: x.lower().translate(str.maketrans('', '', string.punctuation))) \
              .filter(lambda x: x not in stopwords).filter(lambda x: x != '')

  # Map each word to a tuple of (word, 1), and then reduce by key to count the occurrences of each word
  counts = words.map(lambda x: (x, 1)) \
                .reduceByKey(lambda a,b: a + b)
  
  # Sort the word counts in descending order and return the result
  sorted_counts = counts.sortBy(lambda x: x[1], ascending=False)
  return sorted_counts
if __name__ == '__main__':
  from pyspark.context import SparkContext
  sc = SparkContext('local', 'test')
  counts = countWords(sc, "frankenstien.txt","stopwords.txt")
  counts.sortBy(lambda x: x[1], False).saveAsTextFile("output10")