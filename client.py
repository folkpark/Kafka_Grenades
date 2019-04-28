from kafka import KafkaConsumer, KafkaProducer

# consumer = KafkaConsumer('test',
#                          bootstrap_servers='3.95.28.49:9092',
#                          AUTO_OFFSET_RESET_CONFIG = 'earliest')


producer = KafkaProducer(bootstrap_servers='ec2-3-95-28-49.compute-1.amazonaws.com:9092')

for i in range(10):
    msg = "Testing %s" %(str(i))
    producer.send("test", b'msg')
