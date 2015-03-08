#!/usr/bin/python
# -*- coding: utf-8 -*-

# import csv

# results = []
# with open('yelp_academic_dataset_user.json', newline='\n') as inputfile:
#     for row in csv.reader(inputfile):
#         results.append(row)

# 1. import all reviews, find max and min funny


# sample data structure
# {u'business_id': u'vcNAWiLM4dR7D2nwwJ7nCA',
#  u'date': u'2007-05-17',
#  u'review_id': u'15SdjuK7DmYqUAj6rjGowg',
#  u'stars': 5,
#  u'text': u"dr. goldberg offers everything i look for in a general practitioner.  he's nice and easy to talk to without being patronizing; he's always on time in seeing his patients; he's affiliated with a top-notch hospital (nyu) which my parents have explained to me is very important in case something happens and you need surgery; and you can get referrals to see specialists without having to see him first.  really, what more do you need?  i'm sitting here trying to think of any complaints i have about him, but i'm really drawing a blank.",
#  u'type': u'review',
#  u'user_id': u'Xqd0DzHaiyRqVH3WRG7hzg',
#  u'votes': {u'cool': 1, u'funny': 0, u'useful': 2}}



import csv, pickle, json, nltk, re
from nltk.tokenize import RegexpTokenizer
from nltk.corpus import stopwords
from pprint import pprint
import numpy as np
from sklearn.naive_bayes import BernoulliNB

stopwords = stopwords.words('english')
tokenizer = RegexpTokenizer(r'\w+')

def make_vector(popular_words, rev):
  arr = []

  for popword in popular_words:
    if popword[0] in tokenizer.tokenize(rev.lower()):
      arr.append(1)
    else:
      arr.append(0)

  # print arr
  return arr


if __name__ == "__main__":

  funnys = []
  clean_data = []

  with open('yelp_academic_dataset_review.json') as inputfile:
      for i ,row in enumerate(inputfile):

          row_json = json.loads(row)
          # print type(row), len(row), "................", row, row_json
          # pprint(row_json)
          currentFunny = row_json['votes']['funny']
          review_text = row_json['text']
          if currentFunny != 0:
            funnys.append(currentFunny)
            # test = np.array([str(review_text.encode('ascii', 'ignore')), int(currentFunny), -1])
            # print test
            clean_data.append([str(review_text.encode('ascii', 'ignore')), int(currentFunny)])

          # if i == 3:
          #   break

          # if currentFunny == 90:
          #   print review_text
          #   print row_json

          if len(funnys) == 100000:
            break

  
  # pickle.dump( funnys, open( "funnyData.p", "wb" ) )
  # print funnys
  # funnys = pickle.load( open( "funnyData.p", "rb" ) )

  maxFunnys = max(funnys)
  minFunnys = min(funnys)
  print "max funnys: ", max(funnys)
  print "min funnys: ", min(funnys)

  funnyThreshold = minFunnys + 0.75 * (maxFunnys - minFunnys )

  print "funnyThreshold:", funnyThreshold

  # print clean_data

  # dt = np.dtype([('review_text', np.str_, 1000), ('funnyScore', np.int8), ('isFunny', np.int8 )])
  # clean_data = np.array(clean_data, dtype = dt)

  tempdata = []
  rawFunnyTextReviews = []
  for i in clean_data:
    if i[1] >= funnyThreshold:
      tempdata.append([i[0],1])
      rawFunnyTextReviews.append(i[0])
    else:
      tempdata.append([i[0],0])
    

  clean_data= tempdata

  # print clean_data

  # find top 50 most popular word

  # print rawFunnyTextReviews

  funnyReviews_str = ' '.join(rawFunnyTextReviews).lower()





  # word_tokens =  nltk.word_tokenize(funnyReviews_str)
  word_tokens = tokenizer.tokenize(funnyReviews_str)

  
  word_tokens = [w for w in word_tokens if w not in stopwords]

  fdist = nltk.FreqDist(word_tokens)


  popular_words = fdist.most_common(50)


  print popular_words
  rev = 'i go go go leave one.'

  X = []
  Y = []
  for i in clean_data:
    vecX = make_vector(popular_words, i[0])
    X.append(vecX)
    Y.append(i[1])

  # print X
  # print Y
  print len(Y)

  clf = BernoulliNB()
  clf.fit(X, Y)

  pickle.dump( clf, open( "model.p", "wb" ) )
  pickle.dump( popular_words, open( "popular_words.p", "wb" ) )

  test_str = "i have a dream"
  testX = make_vector(popular_words ,test_str)
  print clf.predict(testX)




  # arr = []

  # for popword in popular_words:
  #   if popword[0] in tokenizer.tokenize(rev):
  #     arr.append(1)
  #   else:
  #     arr.append(0)

  # print arr 









  




