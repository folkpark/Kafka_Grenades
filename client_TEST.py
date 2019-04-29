from kafka import KafkaConsumer, KafkaProducer
import threading
import time

# msg = "Producing %s" % (str(i))
# msg = msg.encode('utf-8')
# producer.send('test', msg).get(timeout=30)


def printMenu():
    print("\n Enter integer selection (q to quit)): ")
    print("Update Location 1: ")
    print("Throw Grenade 2: ")
    choice_int = input("Selection: ")
    return choice_int

def producer():
    producer = KafkaProducer(bootstrap_servers=['3.95.28.49:9092'])
    while True:
        choice_int = printMenu()
        if choice_int is '1':
            newLoc = input("New location = ")
            newLoc = int(newLoc)
            print("Updating Location to %d" %newLoc)
            # Update Location HERE
        elif choice_int is '2':
            print("Throwing Grenade ... ")
            # Grenade Throw logic HERE
        else:
            print("Good Bye!")
            break



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