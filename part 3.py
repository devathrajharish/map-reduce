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
def newfunc(y):
    lst = []
    for t in y:
        lst.append(y[t][1])
    value = []
    size = len(lst)
    for i in range(size):
        for j in range(i+1, size):
            value.append((lst[i][0]+lst[j][0], lst[i][1]*lst[j][1]))
    return lst
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
    output = sc.parallelize(rdd2)
    output.persist()
    pdd = rdd2.map(lambda l:(l[0], l[1])).map(lambda (x, y): (x, y[1]))
    pdd1 pdd.map(lambda (x, y) : newfunc(y)).flatMap(lambda x: x).reduceByKey(lambda x,y : x + y)
    pdd 2 = sc.parallelize(pdd1)


https://github.com/neoluc/Spark-SQL-Inverted-Index-Search-Engine/blob/master/inverted%20index%20search%20sql.py
