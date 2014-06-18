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

def geotweets(c):
  """ get a geojson list of points where tweets were tweeted in BE"""
  lst = []
  for tweet in c.find({'place': {'$ne': 'null'}}):
    try:
      place = tweet['place']
      if place['country_code'] == 'BE':
        point = findCenter(place['bounding_box']['coordinates'][0])
        geojson = {"type": "Feature",
                   "properties": 
                     {
                      "name": str(tweet['user']['screen_name']).strip(),
                      "amenity": "Tweet",
                      "popupContent": str(tweet['user']['screen_name']).strip()
                     },
                  "geometry": 
                    {
                      "type": "Point",
                      "coordinates": [point[0], point[1]]
                    }
                  }
        lst.append(geojson)
    except:
      continue
  return lst

def findCenter(coordinates):
  """ simple averaging approach to find the center of a polygon, awful approach """
  longs = []
  lats = []
  for coordinate in coordinates:
    longs.append(coordinate[0])
    lats.append(coordinate[1])
  return [sum(longs)/float(len(coordinates)), sum(lats)/float(len(coordinates))]

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
#  write( tweetsPerLevel(mongocollection, 'user.screen_name'), './users.freq.tab' )
  write( geotweets(mongocollection), './locations.geojson' )

if __name__ == '__main__':
  main()

