import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

def create_context(host, port):
    print "Creating new context"
    sc = SparkContext(appName="StreamingWordCount")
    ssc = StreamingContext(sc, 2)

    lines = ssc.socketTextStream(host, port)

    def countWords(newValues, lastSum):
      if lastSum is None:
        lastSum = 0
      return sum(newValues, lastSum)  

    word_counts = lines.flatMap(lambda line: line.split(" "))\
                  .filter(lambda w: w.startswith("#"))\
                  .map(lambda word: (word, 1))\
                  .updateStateByKey(countWords)

    word_counts.pprint()

    return ssc


if __name__ == "__main__":

    host, port, checkpoint_dir = sys.argv[1:]

    print checkpoint_dir
    ssc = StreamingContext.getOrCreate(checkpoint_dir,
                                       lambda: create_context(host, int(port)))    
    ssc.start()
    ssc.awaitTermination()
