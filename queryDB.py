###############################################################################
# queryDB.py provides functions for querying the local mongoDB that contains 
# the Twitter messages that were collected.
###############################################################################

import pymongo, re, time, sys, codecs
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

def tweetsPerLevel(c, variable):
  """ return a column with amount of tweets per distinct user """
  qout = c.aggregate(
    { "$group": {
        "_id": "$" + variable,
        "tweetsPerLevel": { "$sum": 1 }
    }}
  )
  out = ['freq\tuser']
  # bit of slow hack to get this in a column format
  for item in qout['result']:
    out.append( str(item['tweetsPerLevel']) + '\t' + unicode(item['_id']) )
  return '\n'.join(out)

def write(s, fname):
  fout = codecs.open(fname, 'w', 'utf-8')
  fout.write( unicode(s) )
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
#  write( totalNumberOfTweets(mongocollection), './tweets.tab' )
#  write( totalNumberOfUsers(mongocollection), './users.tab' )
  write( tweetsPerLevel(mongocollection, 'user.screen_name'), 'users.freq.tab' )

if __name__ == '__main__':
  main()

