import time, re
from pyspark import SparkContext, SparkConf

def linesToWordsFunc(line):
    wordsList = line.lower().split()
    wordsList = [re.sub(r'\W+', '', word) for word in wordsList]
   # filtered = filter(lambda word: re.match(r'\w+', word), wordsList)
    filteragain = filter(lambda x: len(x) > 3, wordsList )
    return filteragain

def main():
    conf = SparkConf().setAppName("Words count")
    conf.set("spark.executor.memory", "2g")
    sc = SparkContext(conf=conf)
    rdd = sc.textFile("/cosc6339_hw2/gutenberg-22/*.txt")
    words = rdd.flatMap(linesToWordsFunc)
    pairs = words.map(lambda word: (word, 1))
    counts = pairs.reduceByKey(lambda a, b : a + b)
    output = counts.takeOrdered(1000, lambda(k, v): -v)
    new = sc.parallelize(output)
    .saveAsTextFile('outpart.txt')

    sc.stop()

if __name__ == "__main__":
    main()