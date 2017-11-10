import pymongo
from datetime import datetime as dt
import pytz
from SETTINGS import database, collection
connection = pymongo.MongoClient('localhost', 27017)
db = connection[database][collection]
cursor = db.find({'retweeted_status':{'$exists':True}}, no_cursor_timeout=True)
total = cursor.count()
counter = 0
for x in cursor:
    if counter % 10000 == 0:
        print('Extracted {} of {} Retweets'.format(counter,total))
    # print(type(x['retweeted_status']))
    # print(x['retweeted_status'])
    connection[database][collection].update_one({'id': x['id']}, {'$setOnInsert':x['retweeted_status']}, upsert=True)
    counter += 1