
from kafka import KafkaProducer
import json

producer = KafkaProducer(bootstrap_servers='localhost:9092')
# for i in range(1):
#     a = producer.send('vutriantest', key=b'message-two', value=b'This is Kafka-Python deptry')


with open('data_keyword/10001.json') as data_file:    
    mydata = json.load(data_file)

for data in mydata[:100]:

    print('Producing message: %s' % data)
    a = producer.send('kafka-mongo', json.dumps(data).encode('utf-8'))

producer.flush()
print("send_messsage_ok")

print(a.succeeded())