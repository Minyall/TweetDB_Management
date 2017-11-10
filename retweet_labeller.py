import pymongo
from datetime import datetime as dt
import pytz
from SETTINGS import database, collection
connection = pymongo.MongoClient('localhost', 27017)
db = connection[database][collection]
cursor = db.find(no_cursor_timeout=True)
total = cursor.count()
counter = 0
for x in cursor:
    if counter % 10000 == 0:
        print('Extracted {} of {} Retweets'.format(counter,total))
    # print(type(x['retweeted_status']))
    if 'retweeted_status' in x:
        switch = True
    else:
        switch = False
    db.update_one({'id': x['id']}, {'$set':{'is_retweet':switch}})
    counter += 1