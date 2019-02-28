import time, re
from pyspark import SparkContext, SparkConf
from pyspark import join
from operator import add
import ntpath

def linesToWordsFunc(line):
    wordsList = line[1].lower().split()
    wordsList = [re.sub(r'\W+', '', word) for word in wordsList]
   # filtered = filter(lambda word: re.match(r'\w+', word), wordsList)
    filteragain = filter(lambda x: len(x) > 3, wordsList )
    return filteragain

def main():
    conf = SparkConf().setAppName("Words count")
    conf.set("spark.executor.memory", "2g")
    sc = SparkContext(conf=conf)
    rdd2 = sc.wholeTextFiles("/cosc6339_hw2/gutenberg-22/*.txt")\
                .flatMap(lambda (name, content): map(lambda word: (word, ntpath.basename(name)), content.lower().split()))\
                .map(lambda (word, name): ((word, name), 1))\
                .reduceByKey(lambda count1, count2: count1 + count2)\
                .map(lambda ((word, name), count): (word, [name, count]))\
                .reduceByKey(lambda a,b: a+b)


   # rdd2.saveAsTextFile('/bigd30/harishoutt.txt')
    output = rdd2.collect()
    for i in range(100):
       print output[i]
    sc.stop()

if __name__ == "__main__":
    main()