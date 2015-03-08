#!/usr/bin/python
# -*- coding: utf-8 -*-
from pyspark import SparkContext, SparkConf
# this is the location of the cluster
master = "spark://ec2-52-10-98-37.us-west-2.compute.amazonaws.com:7077"
appName = "DataScience HW1 CT"

conf = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=conf)

def split_and_cast(x):
  r = x.split()[0:3]
  r = tuple([r[0],int(r[1]),int(r[2])])
  return r


class WordFreqCluster:
  """HW1 Task2, examine unigram frequencies, find those with highly correlated frequencies and those with low correlated frequencies"""
  def __init__(self):
    # self.ngramsFile = "googlebooks-eng-all-1gram-20120701-a"  # Should be some file on HDFS
    self.ngramsFile = "sampledata.txt"
    self.ngramsData = sc.textFile(self.ngramsFile).cache()

    # data: ngram TAB year TAB match_count TAB page_count TAB volume_count NEWLINE
    # sample line: circumvallate   1978   335    91


  def topWordFrequencies(self):
    r = self.ngramsData.map(split_and_cast) \
                        .filter(lambda x: x[1] > 1980 ).cache()

    self.recentngrams= r

    r =r.map(lambda x: x[0::2]) \
                        .reduceByKey(lambda x,y: x+y) \
                        .map(lambda x:(x[1],x[0])) \
                        .sortByKey(True) \
                        .map(lambda x:(x[1],x[0])) \
                        .take(5)


    self.topngrams = sc.parallelize(r)

    return r


  def groupByWord(self):
    topngrams = self.topngrams
    print "topngrams type:", type(topngrams), topngrams

    recentngrams = self.recentngrams.map(lambda x: (x[0],(x[1], x[2])) )
    r = recentngrams.groupByKey() \
                    .join(topngrams) \
                    .map(lambda x: (x[0], x[1][0])) \
                    
    r_join_r = r.cartesian(r) \
                .filter(lambda x: x[0][0] != x[1][0]) \
                .take(1)

     
    return r_join_r

  def find_correlation(self):
    recentngrams = self.recentngrams




  # def correl(self, word1, word2):

# plan:
# 1. count frequencies of last 50 years group by word, grab 100 most frequent unigrams
# 2. find correl coefficient of 100 by 100 Words
# 3. sort by correl and find top and bottom 20 



if __name__ == "__main__":
  task2 = WordFreqCluster()

  # numAs = logData.filter(lambda s: 'a' in s).count()
  # numBs = logData.filter(lambda s: 'b' in s).count()

  print 'topWordFrequencies:', task2.topWordFrequencies()
  print 'groupByWord:', task2.groupByWord()
  # print 'Øverst_ADV appears: ', task2.wordFrequency('b') 
  # print "Lines with a: %i, lines with b: %i" % (numAs, numBs)