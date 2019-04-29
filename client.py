from kafka import KafkaConsumer, KafkaProducer
import threading
from Node import Node
import time
import os


# msg = "Producing %s" % (str(i))
# msg = msg.encode('utf-8')
# producer.send('test', msg).get(timeout=30)

def printMenu():
    print("\n\nEnter integer selection (q to quit)): ")
    print("Update Location 1: ")
    print("Throw Grenade 2: ")
    print("Get Health: 3")
    choice_int = input("Selection: ")
    print("\n\n")
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
            myNode.set_position(newLoc)
            print("Updated location to %s" %myNode.get_position())
        elif choice_int is '2':
            print("Throwing Grenade ... ")
            # Grenade Throw logic HERE
            # Message format: "<type>,<position thrown to>, <from what client_id>"
            msg = "grenade,%s,%s"%(myNode.get_position(),myNode.get_id())
            msg = msg.encode('utf-8')
            producer.send('test', msg).get(timeout=30)
        elif choice_int is '3':
            print("Your health is = %s"%myNode.get_health())
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
        msgType,position,sender_id = message.split(',')
        if msgType == 'grenade' and sender_id != myNode.get_id():
            #Sub 45 health points from current health
            myNode.set_health(myNode.get_health() - 45)
            #If client is out of health they die
            if myNode.get_health() <= 0:
                print("YOU DIED!!!")
            #Print the Grenade event to the screen
            print("\n\nI was hit with %s, at %s, from %s \n\n" %(msgType,position,sender_id))



if __name__ == "__main__":

    myNode = Node(os.environ['CLIENT_ID'], 3, 100)

    threads_L = []
    producerThread = threading.Thread(target=producer)
    threads_L.append(producerThread)
    consumerThread = threading.Thread(target=consumer)
    threads_L.append(consumerThread)
    consumerThread.start()
    time.sleep(1)
    producerThread.start()