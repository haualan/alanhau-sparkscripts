import csv, pickle, json, nltk, re
from nltk.tokenize import RegexpTokenizer
from nltk.corpus import stopwords
from pprint import pprint
import numpy as np
from sklearn.naive_bayes import BernoulliNB
import funnylyzer


clf = pickle.load( open( "model.p", "rb" ) )
popular_words = pickle.load( open( "popular_words.p", "rb" ) )

#print popular_words


def useModel(iStr):
  vec = funnylyzer.make_vector(popular_words ,iStr)
  r = clf.predict(vec)
  # score = clf.predict_proba(vec)
  if r[0] == 0:
    print "*Yawn*, this is kind of boring. Keep your day job as you aren't a comedian yet.\n"
  else:
    print "lolzzzzzz it's funny...even my mom laughed out loud.\n"



# useModel("This place sucks.")

# useModel("I have a dream. The stripper in vegas and the dancer had never licked the club. I tell ya, babies were starving. The mexican trance music could not be called art and man i had a meal with a side of homicide. That aside, yes, the burger is good.")