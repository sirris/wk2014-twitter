###############################################################################
# queryDB.py provides functions for querying the local mongoDB that contains 
# the Twitter messages that were collected.
###############################################################################

import pymongo, re, time, sys, codecs
from bson.son import SON
from collections import Counter
from datetime import datetime
from bson.code import Code

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

def getByDotNotation( obj, ref ):
  val = obj
  for key in ref.split( '.' ):
    try:
      val = val[key]
    except KeyError:
      continue
    except TypeError:
      try:
        val = val[0][key]
      except KeyError:
        continue
  if isinstance(val, (str, unicode)):
    return val
  else:
    return None

def tweetsPerLevel(c, variable):
  """ return a column with amount of tweets per distinct user, and yeah yeah map reduce """
  qout = [getByDotNotation(item, variable) for item in c.find()]
  qoutc = Counter(qout)
  out = ['freq\tvariable']
  # bit of slow hack to get this in a column format
  for item in qoutc:
    if item:
      out.append( str(qoutc[item]) + '\t' + item )
  return '\n'.join(out)

def flatgeotweets(c):
  """ get a list of points with freq where tweets were tweeted in BE"""
  lst = []
  for tweet in c.find({'place': {'$ne': 'null'}}):
    try:
      place = tweet['place']
      if place['country_code'] == 'BE':
        point = findCenter(place['bounding_box']['coordinates'][0])
        pnt = ', '.join(str( round(x, 4) ) for x in point)
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
  """ yeah yeah map reduce... """
  out = ['freq\tnumber\tname']
  for p in pdb.keys():
    ln = pdb[p]['lastname']
    regex = re.compile(".*" + ln + ".*", re.IGNORECASE)
    hits = c.find({"text": regex})
    f = hits.count()
    out.append(str(f) + '\t' + str(p) + '\t' + pdb[p]['firstname'] + ' ' + ln)
  return '\n'.join(out)

def tokfreqPer10Minutes(c, lng, th):
  """ map reduce sometime """
  db = {}
  fin = open('stoplist_' + lng + '.txt', 'r')
  stoplist = fin.read().lower().split('\n')
  fin.close()
  fin = open('wordsEn.txt', 'r')
  stoplist.extend( fin.read().lower().split('\n') )
  stoplist = set(stoplist)
  fin.close()
  for tweet in c.find():
    try:
      if tweet['lang'] == lng:
        ts = tweet['created_at_hour10minute']
        for w in tweet['text'].lower().split():
          w = re.sub('[!@#$%&\(\)\*:;\.,\?]', '', w)
          if w not in stoplist:
            try:
              db[ts][w] += 1
            except KeyError:
              try:
                db[ts][w] = 1
              except KeyError:
                db[ts] = {w: 1}
    except KeyError:
      continue
  out = ['ts\tword\tfreq']
  for ts in db.keys():
    for word in db[ts].keys():
      if db[ts][word] > th:
        line = '\t'.join([ts, word, str(db[ts][word])])
        out.append(line)
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
  playersdb = readPlayersDB(sys.argv[3])

  # set time
#  addTime( mongocollection)

  # start querying
  print 'total tweets'
#  write( totalNumberOfTweets(mongocollection), './tweets.tab' )

  print 'total users'
#  write( totalNumberOfUsers(mongocollection), './users.tab' )

  print 'tweets per screen name'
#  write( tweetsPerLevel(mongocollection, 'user.screen_name'), './users.freq.tab' )

  print 'tweets per language'
#  write( tweetsPerLevel(mongocollection, 'lang'), './lang.tab' )

  print 'tweets per location'
#  write( geotweets(mongocollection), './locations.geojson' )
#  write( flatgeotweets(mongocollection), './locations.tab' )

  print 'tweets per media'
#  write( tweetsPerLevel(mongocollection, 'entities.media.media_url'), './media.tab' )
 
  print 'tweets per minute'
#  write( tweetsPerLevel(mongocollection, 'created_at_hourminute'), './tweets.minute.tab' )

  print 'tweets per player'
#  write( tweetsPerPlayer(mongocollection, playersdb), './freq.players.tab' )

  print 'token frequency per ten minutes'
  write( tokfreqPer10Minutes(mongocollection, 'nl', 100), './10min.tokfreq.tab' )

if __name__ == '__main__':
  main()

