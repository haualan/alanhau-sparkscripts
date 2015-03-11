#!/usr/bin/python
# -*- coding: utf-8 -*-
from pyspark import SparkContext, SparkConf
import numpy as np
# this is the location of the cluster
master = "spark://ec2-52-11-220-71.us-west-2.compute.amazonaws.com:7077"
# master = "local"
appName = "DataScience HW1 CT"

conf = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=conf)

datafiles = [
# 'googlebooks-eng-all-1gram-20120701-a',
# 'googlebooks-eng-all-1gram-20120701-b',
# 'googlebooks-eng-all-1gram-20120701-c',
# 'googlebooks-eng-all-1gram-20120701-d',
# 'googlebooks-eng-all-1gram-20120701-e', 
# 'googlebooks-eng-all-1gram-20120701-f',
# 'googlebooks-eng-all-1gram-20120701-g',
# 'googlebooks-eng-all-1gram-20120701-h',
# 'googlebooks-eng-all-1gram-20120701-i',
# 'googlebooks-eng-all-1gram-20120701-j',
# 'googlebooks-eng-all-1gram-20120701-k',
# 'googlebooks-eng-all-1gram-20120701-l',
# 'googlebooks-eng-all-1gram-20120701-m',
# 'googlebooks-eng-all-1gram-20120701-n',
# 'googlebooks-eng-all-1gram-20120701-o',
# 'googlebooks-eng-all-1gram-20120701-other',
# 'googlebooks-eng-all-1gram-20120701-p',
# 'googlebooks-eng-all-1gram-20120701-q',
# 'googlebooks-eng-all-1gram-20120701-r',
# 'googlebooks-eng-all-1gram-20120701-s',
# 'googlebooks-eng-all-1gram-20120701-t',
'googlebooks-eng-all-1gram-20120701-u',
'googlebooks-eng-all-1gram-20120701-v',
'googlebooks-eng-all-1gram-20120701-w',
'googlebooks-eng-all-1gram-20120701-x',
'googlebooks-eng-all-1gram-20120701-y',
'googlebooks-eng-all-1gram-20120701-z'
]

def split_and_cast(x):
  r = x.split()[0:3]
  r = tuple([r[0],int(r[1]),int(r[2])])
  return r

def exportToFile(lst, filename):
  fout = open(filename,'w')
  for i in lst:
    fout.write(str(i) + '\n')

  fout.close()



class WordFreqCluster:
  """HW1 Task2, examine unigram frequencies, find those with highly correlated frequencies and those with low correlated frequencies"""
  def __init__(self):

    # self.ngramsFile = "googlebooks-eng-all-1gram-20120701-other"  # Should be some file on HDFS
    # self.ngramsFile = "sampledata.txt"
    # self.ngramsFile = "s3n://alanhau/sampledata.txt"
    # self.ngramsFile = "s3n://alanhau/googlebooks-eng-all-1gram-20120701-a"

    # self.ngramsData = sc.textFile(self.ngramsFile)

    for k, f in enumerate(datafiles):
      singleFile = "s3n://alanhau/" + f 
      singleData = sc.textFile(singleFile)
      if k > 0:
        ngramsData = ngramsData.union(singleData)
      else:
        ngramsData = singleData

    self.ngramsData = ngramsData

    # data: ngram TAB year TAB match_count TAB page_count TAB volume_count NEWLINE
    # sample line: circumvallate   1978   335    91


  def topWordFrequencies(self):
    r = self.ngramsData.map(split_and_cast) \
                        .filter(lambda x: x[1] > 1980 )

    self.recentngrams= r

    # NOte here we are only examining / taking top 100 unigrams to do a correlation matrix.
    r =r.map(lambda x: x[0::2]) \
                        .reduceByKey(lambda x,y: x+y) \
                        .map(lambda x:(x[1],x[0])) \
                        .sortByKey(False) \
                        .map(lambda x:(x[1],x[0])) \
                        .take(500)


    self.topngrams = sc.parallelize(r)

    return r


  def groupByWord(self):
    topngrams = self.topngrams
    # print "topngrams type:", type(topngrams), topngrams

    recentngrams = self.recentngrams.map(lambda x: (x[0],(x[1], x[2])) )
    r = recentngrams.groupByKey() \
                    .join(topngrams) \
                    .map(lambda x: (x[0], x[1][0])) \
                    
    r_join_r = r.cartesian(r) \
                .filter(lambda x: x[0][0] != x[1][0]).cache()

    self.r_join_r = r_join_r

    # print "r_join_r count:", r_join_r.count()

    return r_join_r.collect()

  def map_correlation(self):
    r_join_r = self.r_join_r
    r = r_join_r.map(lambda x: find_correlation(x)) \
                .map(lambda x: (x[1],(x[0],x[2]))) \
                .cache() 

    # r.saveAsTextFile('~/correl.txt')

    asc_correl = r.sortByKey(True).take(1000)
    # print 'ascending correl', asc_correl
    exportToFile(asc_correl,'asc_correl.txt')


    desc_correl = r.sortByKey(False).take(1000)
    # print 'descending correl', desc_correl
    exportToFile(desc_correl, 'desc_correl.txt')

def find_correlation(row):
  try:
    leftword_pyspark_arr = row[0][1]
    rightword_pyspark_arr = row[1][1]

    leftword_arr = []
    rightword_arr = []

    # XY is a list of intersection points where both words have non-zero frequency
    XY = np.array([(x[1], y[1]) for x in leftword_pyspark_arr for y in rightword_pyspark_arr if x[0] == y[0]])
    # print "XY", XY

  
    X = XY[:,0]
    Y = XY[:,1]

    if len(XY) > 2:
      r = np.corrcoef(X, Y)[0][1]
    else:
      r = 0

    if np.isnan(r):
      r = 0
  except:
      r = 0
      XY = []


  # print r, row[0][0], row[1][0]

  # generate a unique word pair as key so we can run distinct on RDD
  if row[0][0] > row[1][0]:
    key =  row[0][0] + '|' + row[1][0]
  else:
    key =  row[1][0] + '|' + row[0][0]

  # return key, r, len(XY), XY.tolist()
  return key, r, len(XY)


# plan:
# 1. count frequencies of last 50 years group by word, grab 100 most frequent unigrams
# 2. find correl coefficient of 100 by 100 Words
# 3. sort by correl and find top and bottom 20 



if __name__ == "__main__":
  task2 = WordFreqCluster()

 

  print 'topWordFrequencies:', task2.topWordFrequencies()

  r_join_r = task2.groupByWord()
  print 'groupByWord:'
  # , r_join_r
  # print 'Øverst_ADV appears: ', task2.wordFrequency('b') 
  # print "Lines with a: %i, lines with b: %i" % (numAs, numBs)

  results = []
  for v in r_join_r:
    results.append(find_correlation(v))

  exportToFile(results,'all_correl.txt')

  print "map_correlation", task2.map_correlation()




