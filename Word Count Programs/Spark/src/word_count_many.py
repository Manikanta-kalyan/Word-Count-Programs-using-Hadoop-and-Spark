
#sc.stop()
import string

# Define function to count words and perform additional requirements
def countWords(sc, files, stopwords_file):
  # Load stop words file into a Python list
  with open(stopwords_file) as f:
    stopwords = [word.strip() for word in f]
  
  # Create an RDD from each file, and apply word count and filtering operations to each RDD and reduce each RDD  and union the RDD counts
  word_counts = None
  for file in files:
    lines = sc.textFile(file)
    words = lines.flatMap(lambda x: x.split(' ')) \
                .map(lambda x: x.lower().replace('“','').translate(str.maketrans('', '', string.punctuation))) \
                .filter(lambda x: x not in stopwords) \
                .filter(lambda x: x != '')
    counts = words.map(lambda x: (x, 1)).reduceByKey(lambda a,b: a + b)
    if(word_counts==None):
        word_counts=counts
    else:
        word_counts = word_counts.union(counts)
  
  # use reduceByKey() to get a single RDD of word counts
  combined_counts = word_counts.reduceByKey(lambda a,b: a + b)
  
  # Sort the word counts in descending order
  sorted_counts = combined_counts.sortBy(lambda x: x[1], ascending=False)
  
  # Write the sorted word counts to the output file
  return sorted_counts

if __name__ == '__main__':
  from pyspark.context import SparkContext
  sc = SparkContext('local', 'test')
  counts = countWords(sc, ['aroomwithaview.txt','bluecastle.txt','enchantedapril.txt','frankenstien.txt','littlebeth.txt','middlemarch.txt','mobydick.txt','prideandpredijuice.txt','romandjul.txt','twentyyearsafter.txt'],"stopwords.txt")
  counts.sortBy(lambda x: x[1], False).saveAsTextFile("outputmany7")