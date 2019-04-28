from kafka import KafkaConsumer, KafkaProducer

# consumer = KafkaConsumer('test',
#                          bootstrap_servers='3.95.28.49:9092',
#                          AUTO_OFFSET_RESET_CONFIG = 'earliest')


producer = KafkaProducer(bootstrap_servers=['3.95.28.49:9092'])

for i in range(10):
    msg = "Testing %s" %(str(i))
    msg = msg.encode('utf-8')
    producer.send('test', msg).get(timeout=30)
    print("Sending %s"%msg)
