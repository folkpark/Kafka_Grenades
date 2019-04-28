from kafka import KafkaConsumer, KafkaProducer
import threading
import time

def producer():
    producer = KafkaProducer(bootstrap_servers=['3.95.28.49:9092'])
    while True:
        for i in range(5):
            msg = "Producing %s" %(str(i))
            msg = msg.encode('utf-8')
            producer.send('test', msg).get(timeout=30)
            print("Sending %s"%msg)
        time.sleep(3)


#	AUTO_OFFSET_RESET_CONFIG = 'earliest' is used if consumers need to look
# back through the queue
def consumer():
    consumer = KafkaConsumer('test', bootstrap_servers=['3.95.28.49:9092'])
    # Should be infinite loop
    for messages in consumer:
        message = messages.value.decode("utf-8")
        print("I just consumed: %s" %message)



if __name__ == "__main__":
    threads_L = []
    producerThread = threading.Thread(target=producer)
    threads_L.append(producerThread)
    consumerThread = threading.Thread(target=consumer)
    threads_L.append(consumerThread)
    consumerThread.start()
    time.sleep(1)
    producerThread.start()