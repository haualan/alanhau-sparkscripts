import csv, pickle, json, nltk, re
from nltk.tokenize import RegexpTokenizer
from nltk.corpus import stopwords
from pprint import pprint
import numpy as np
from sklearn.naive_bayes import BernoulliNB
import plotly.plotly as py
from plotly.graph_objs import *

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

  fans = []
  comps = []

  with open('yelp_academic_dataset_user.json') as inputfile:
      for i ,row in enumerate(inputfile):

          row_json = json.loads(row)
          # print type(row), len(row), "................", row, row_json
          # pprint(row_json)

          # currentFunny = row_json['votes']['funny']
          # review_text = row_json['text']
          # if currentFunny != 0:
          #   funnys.append(currentFunny)
          #   # test = np.array([str(review_text.encode('ascii', 'ignore')), int(currentFunny), -1])
          #   # print test
          #   clean_data.append([str(review_text.encode('ascii', 'ignore')), int(currentFunny)])

          # # if i == 3:
          # #   break

          # # if currentFunny == 90:
          # #   print review_text
          # #   print row_json

          count_fans = row_json['fans']
          try: 
            funny_compliments = row_json['compliments']['funny']
          except:
            funny_compliments = 0

          fans.append(count_fans)
          comps.append(funny_compliments)

          # if i == 1:
          #   break



  trace1 = Scatter(
      x=fans,
      y=comps,
      mode='markers'
  )


  data = Data([trace1])
  plot_url = py.plot(data, filename='scatter')