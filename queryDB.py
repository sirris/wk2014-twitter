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
  out = ['freq\tvariable']
  # bit of slow hack to get this in a column format
  for item in qout['result']:
    out.append( str(item['tweetsPerLevel']) + '\t' + unicode(item['_id']) )
  return '\n'.join(out)

def flatgeotweets(c):
  """ get a list of points with freq where tweets were tweeted in BE"""
  lst = []
  for tweet in c.find({'place': {'$ne': 'null'}}):
    try:
      place = tweet['place']
      if place['country_code'] == 'BE':
        point = findCenter(place['bounding_box']['coordinates'][0])
        pnt = ', '.join(str(x) for x in point)
        lst.append(pnt)
    except:
      continue
  cntr = Counter(lst)
  out = []
  for cnt in cntr:
    out.append( cnt + ', ' + str(cntr[cnt]) )
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

def addTime(c):
  for doc in c.find():
    try:
      ts = doc["created_at"]
      c.update({"_id" : doc["_id"]}, {"$set" : {"created_at_parsed" : datetime.strptime(re.sub(r"[+-]([0-9])+", "", ts),"%a %b %d %H:%M:%S %Y")}})
      hourminute = ':'.join(ts.split(' ')[3].split(':')[0:2])
      c.update({"_id" : doc["_id"]}, {"$set" : {"created_at_hourminute" : hourminute}})
      hour10minute = ts.split(' ')[3].split(':')[0] + ':' + ts.split(' ')[3].split(':')[1][0] + '0'
      c.update({"_id" : doc["_id"]}, {"$set" : {"created_at_hour10minute" : hour10minute}})
    except KeyError:
      continue

def readPlayersDB(fname):
  fin = codecs.open(fname, 'r', 'utf-8')
  lines = fin.readlines()
  fin.close()
  db = {}
  for line in lines:
    l = line.strip().split(';')
    key = l[0]
    db[key] = {'firstname': l[1],
               'lastname': l[2],
               'twitter': l[3]}
  return db

def tweetsPerPlayer(c, pdb):
  out = ['freq\tnumber\tname']
  for p in pdb.keys():
    ln = pdb[p]['lastname']
    regex = re.compile(".*" + ln + ".*", re.IGNORECASE)
    hits = c.find({"text": regex})
    f = hits.count()
    out.append(str(f) + '\t' + str(p) + '\t' + pdb[p]['firstname'] + ' ' + ln)
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

  # read in players db
  playersdb = readPlayersDB('players_nl.txt')

  # start querying
#  write( totalNumberOfTweets(mongocollection), './tweets.tab' )
#  write( totalNumberOfUsers(mongocollection), './users.tab' )
#  write( tweetsPerLevel(mongocollection, 'user.screen_name'), './users.freq.tab' )
#  write( tweetsPerLevel(mongocollection, 'lang'), './lang.tab' )
#  addTime( mongocollection )
#  write( geotweets(mongocollection), './locations.geojson' )
#  write( flatgeotweets(mongocollection), './locations.tab' )
#  write( tweetsPerLevel(mongocollection, 'entities.media.media_url'), './media.tab' )
#  write( tweetsPerLevel(mongocollection, 'created_at_hourminute'), './tweets.minute.tab' )
  write( tweetsPerPlayer(mongocollection, playersdb), './freq.players.tab' )

if __name__ == '__main__':
  main()

