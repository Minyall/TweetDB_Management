import pymongo
from datetime import datetime as dt
import pytz
from SETTINGS import database, collection
connection = pymongo.MongoClient('localhost', 27017)
db = connection[database][collection]
cursor = db.aggregate(pipeline=[{'$match':{'retweeted_status':{'$exists':1}}}])
# total = cursor.count()

for i, x in enumerate(cursor):
    if i % 10000 == 0:
        print('Extracted {} Retweets'.format(i))
    # print(type(x['retweeted_status']))
    # print(x['retweeted_status'])
    connection[database][collection].update_one({'id': x['id']}, {'$setOnInsert':x['retweeted_status']}, upsert=True)
    connection[database][collection].update_one({'id': x['id']},
    {'$max':{'retweet_count':x['retweeted_status']['retweet_count']}})