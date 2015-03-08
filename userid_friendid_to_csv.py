
import csv, pickle, json
from pprint import pprint
import numpy as np

def friends_to_csv(x):
  r = ''
  sym = '","'
  r = '"' + sym.join(x) + '"'
  return r


if __name__ == "__main__":

  result = []

  fout = open('userid_friendid.csv','w')

  with open('yelp_academic_dataset_user.json') as inputfile:
      for i ,row in enumerate(inputfile):

          row_json = json.loads(row)
          # print type(row), len(row), "................", row, row_json
          # pprint(row_json)
          userid = row_json["user_id"]
          friends = row_json["friends"]
          friends_count = len(friends)
          line =  '"' + userid + '"' + ',' + str(friends_count) +',' + friends_to_csv(friends) + '\n'
          fout.write(line)


          


          # if i == 3:
          #   break

  fout.close()





