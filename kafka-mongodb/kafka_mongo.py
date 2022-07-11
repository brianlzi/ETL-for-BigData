import json
import pymongo
import sys
from  pymongo import MongoClient
from kafka import KafkaConsumer
import time
MIN_COMMIT_COUNT = 1000
time.sleep(60)
# extract command line args: host, port and database


# host  = sys.argv[1]
# port  = int(sys.argv[2])
# database = sys.argv[3]
database = "keyword"
# '''
# Set up MongoDB Client
# '''
client = MongoClient("mongodb://mongo1:27017,mongo2:27018,mongo3:27019")
db = client[database]
coll = db.google_search_keyword
consumer = KafkaConsumer('kafka-mongo',group_id ='group1',bootstrap_servers='kafka2:19092')

'''
Kafka Consumer settings
'''
# c = Consumer({'bootstrap.servers': 'localhost:9091', 
#               'group.id': 'mygroup'})
# c.subscribe(['topic_json'])
                
def aggregation_basic(msgs):
    coll.insert_many(msgs)

def consume():
    try:
        i = 0
        msg_count = 0

        for msg in consumer:
            print(msg_count)
            msgs = []
            print('Received message: %s' % msg.value.decode('utf-8'))
            data = json.loads(msg.value.decode("utf-8"))
            data_convert = {}
            data_convert['keyword'] = data["1"]
            data_convert['search_average_month'] = data['1000'] if '1000' in data.keys() else 0
            data_convert['search_average_12_month'] = data['1021'] if '1021' in data.keys() else [0,0,0,0,0,0,0,0,0,0,0,0]
            data_convert['compete'] = data['1001'] if '1001' in data.keys() else 0
            data_convert['low_bid'] = data['1011'] if '1011' in data.keys() else 0
            data_convert['hight_bid'] = data['1012'] if '1012' in data.keys() else 0
            msgs.append(data_convert)
            msg_count += 1
            aggregation_basic(msgs)
                # if msg_count % MIN_COMMIT_COUNT == 0:
                #     consumer.commit()
            msgs=[]
            
    except Exception as e:
        print(e)

def main():
    consume()

if __name__ == "__main__": 
    main()
