import time, re
from pyspark import SparkContext, SparkConf
from pyspark import join
from operator import add
import ntpath
import numpy as np

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
                .map(lambda ((word, name), count): (word, [(name, count)]))\
                .reduceByKey(lambda a,b: a+b)

   # rdd2.saveAsTextFile('/bigd30/harishoutt.txt')
    output = rdd2.collect()
    similarity = np.zeros((22,22))
    k = 0
    doc_list = sc.wholeTextFiles("/cosc6339_hw2/gutenberg-22/*.txt")\
                .flatMap(lambda(name, content): [ntpath.basename(name)]).collect()
    doc_map = {}
    for name in doc_list:
        doc_map[name] = k
        k += 1

    output5 = output[:5]
    for line in output:
        list = line[1]
        for i in range(0, len(list) - 1):
            for j in range(i+1, len(list)):
                doc1 = doc_map[list[i][0]]
                doc2 = doc_map[list[j][0]]
                if doc1 < doc2:
                    similarity[doc1][doc2] += (list[i][1] * list[j][1])
                else:
                    similarity[doc2][doc1] += (list[i][1] * list[j][1])


    sc.stop()

if __name__ == "__main__":
    main()