#!/usr/bin/python
# -*- coding: utf-8 -*-
from pyspark import SparkContext, SparkConf
import numpy as np
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
                .filter(lambda x: x[0][0] != x[1][0]).cache()

    self.r_join_r = r_join_r

    return r_join_r.take(1)

  def map_correlation(self):
    r_join_r = self.r_join_r
    r = r_join_r.map(lambda x: find_correlation(x)).take(5)

    print r

def find_correlation(row):
  leftword_pyspark_arr = row[0][1]
  rightword_pyspark_arr = row[1][1]

  leftword_arr = []
  rightword_arr = []

  # XY is a list of intersection points where both words have non-zero frequency
  XY = np.array([(x[1], y[1]) for x in leftword_pyspark_arr for y in rightword_pyspark_arr if x[0] == y[0]])
  # print "XY", XY
  X = XY[:,0]
  Y = XY[:,1]

  r = np.corrcoef(X, Y)[0][1]


  # print r, row[0][0], row[1][0]

  # generate a unique word pair as key so we can run distinct on RDD
  if row[0][1] > row[1][0]:
    key =  row[0][0] + '|' + row[1][0]
  else:
    key =  row[1][0] + '|' + row[0][0]

  return key, r, len(XY)


    # # convert leftword_pyspark_arr to a numpy array
    # for v in leftword_pyspark_arr:
    #   leftword_arr.append(v)

    # leftword_arr = np.array(leftword_arr)
    # print leftword_arr[:,0]

    # # convert rightword_pyspark_arr to a numpy array
    # for v in rightword_pyspark_arr:
    #   rightword_arr.append(v)

    # rightword_arr = np.array(rightword_arr)
    # print rightword_arr[:,0]

    # # gather unique years
    # uniqueYears = np.intersect1d(leftword_arr[:,0],rightword_arr[:,0])









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

  r_join_r = task2.groupByWord()
  print 'groupByWord:', r_join_r
  # print 'Øverst_ADV appears: ', task2.wordFrequency('b') 
  # print "Lines with a: %i, lines with b: %i" % (numAs, numBs)

  print find_correlation(r_join_r[0])

  print "map_correlation", task2.map_correlation()




