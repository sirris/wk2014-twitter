###############################################################################
# queryDB.py provides functions for querying the local mongoDB that contains 
# the Twitter messages that were collected.
###############################################################################

import pymongo, re, time, sys
from bson.son import SON
from collections import Counter
from datetime import datetime

dbname = sys.argv[1]
cname = sys.argv[2]

client = pymongo.MongoClient("mongodb://localhost:27017")
db = client[dbname]
mongocollection = db[cname]

def totalNumberOfTweets(c):
  """ return column with total number of tweets """
  out = ['tweets']
  out.append( str(c.count()) )
  return '\n'.join(out)

def totalNumberOfUsers(c):
  """ return column with total number of distinct users """
  out = ['users']
  out.append( str(len(c.find().distinct("user.screen_name"))) )
  return '\n'.join(out)

def write(s, fname):
  fout = open(fname, 'w')
  fout.write(s)
  fout.close()

def main():
  # parse the sys args
  dbname = sys.argv[1]
  cname = sys.argv[2]

  # set the database and collection
  client = pymongo.MongoClient("mongodb://localhost:27017")
  mongodb = client[dbname]
  mongocollection = mongodb[cname]

  # start querying
  write( totalNumberOfTweets(mongocollection), './tweets.tab' )
  write( totalNumberOfUsers(mongocollection), './users.tab' )

if __name__ == '__main__':
  main()

