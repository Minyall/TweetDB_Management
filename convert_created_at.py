from mongo_proxy import MongoProxy
from SETTINGS import collection
import pymongo
from mongo_credentials import mongo_loc, mongo_port, mongo_auth_db, mongo_auth_pw, mongo_auth_user, mongo_db
from datetime import datetime as dt
import pytz

#  To be run on server with a local mongo db.

connection = pymongo.MongoClient(mongo_loc, mongo_port)
# connection[mongo_auth_db].authenticate(mongo_auth_user, mongo_auth_pw, mechanism='SCRAM-SHA-1')
db = connection[mongo_db][collection]


counter = 0

for x in db.find():
    if counter % 1000 == 0:
        print('[*] Dated {} records so far...'.format(counter))
    db.update_one({'_id': x['_id']},{'$set':
        {
        'new_created_at': dt.strptime(x['created_at'],'%a %b %d %H:%M:%S +0000 %Y').replace(tzinfo=pytz.UTC),
        'user.new_created_at': dt.strptime(x['user']['created_at'],'%a %b %d %H:%M:%S +0000 %Y').replace(tzinfo=pytz.UTC)
        }
    }

    )
    counter += 1