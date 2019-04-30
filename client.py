from kafka import KafkaConsumer, KafkaProducer
import threading
from Node import Node
import time
import os
from GrenadeGame import GameSetup, Grenade


# msg = "Producing %s" % (str(i))
# msg = msg.encode('utf-8')
# producer.send('test', msg).get(timeout=30)

class Player:

    def __init__(self, node, broker):
        self.MyNode = node
        self.broker = broker
        self.stop = False

    def printMenu(self):
        print("\n\nEnter integer selection (q to quit)): ")
        print("Update Location 1: ")
        print("Throw Grenade 2: ")
        print("Get Health: 3")
        choice_int = input("Selection: ")
        print("\n\n")
        return choice_int

    def producer(self):
        producer = KafkaProducer(bootstrap_servers=self.broker)
        while self.stop is False:
            choice_int = self.printMenu()
            if choice_int is '1':
                newLoc = input("New location = ")
                newLoc = int(newLoc)
                print("Updating Location to %d" % newLoc)
                # Update Location HERE
                myNode.set_position(newLoc)
                message = "{} {} {} {}".format(self.MyNode.id, self.MyNode.health, self.MyNode.x, self.MyNode.y).encode('utf8')
                producer.send(self.MyNode.id, message)
                print("Updated location to %s" % myNode.get_position())
            elif choice_int is '2':
                print("Throwing Grenade ... ")
                # Grenade Throw logic HERE
                # Message format: "<type>,<position thrown to>, <from what client_id>"
                direction = input('Enter a direction(0, 90, 180, 270): ')
                velocity = input('Velocity(0-5): ')
                grenade = Grenade(str(self.MyNode.id), str(myNode.x),str(myNode.y),str(velocity),
                                  str(direction),producer)
                grenade.grenade_throw()

                # msg = "grenade,%s,%s"%(myNode.get_position(), myNode.get_id())
                # msg = msg.encode('utf-8')
                # producer.send('grenade', msg)
            elif choice_int is '3':
                print("Your health is = %s" % myNode.get_health())
            else:
                print("Good Bye!")
                producer.close()
                self.stop = True
                break

    # AUTO_OFFSET_RESET_CONFIG = 'earliest' is used if consumers need to look
    # back through the queue
    def consumer(self, topic):

        while self.stop is False:
            consumer = KafkaConsumer(topic, bootstrap_servers=self.broker, consumer_timeout_ms=15000)
            # Should be infinite loop
            for messages in consumer:
                message = messages.value.decode("utf-8")
                msgType,position,sender_id = message.split(',')
                if msgType == 'grenade' and sender_id != myNode.get_id():
                    # Sub 45 health points from current health
                    myNode.set_health(myNode.get_health() - 45)
                    # If client is out of health they die
                    if myNode.get_health() <= 0:
                        print("YOU DIED!!!")
                    # Print the Grenade event to the screen
                    print("\n\nI was hit with %s, at position %s, from %s \n\n" % (msgType, position, sender_id))


if __name__ == "__main__":

    # client_id = os.environ['CLIENT_ID']
    client_id = '1'
    broker = 'ec2-3-95-28-49.compute-1.amazonaws.com:9092'
    myNode = Node(client_id, 3, 3, 100)
    #myNode = Node(str(client_id), 3, 3, 100)
    GameSetup(broker, myNode)
    player = Player(myNode, broker)

    threads_L = []
    producerThread = threading.Thread(target=player.producer())
    threads_L.append(producerThread)
    consumerThread1 = threading.Thread(target=player.consumer('grenade'))
    threads_L.append(consumerThread1)
    consumerThread1.start()
    consumerThread2 = threading.Thread(target=player.consumer(client_id))
    threads_L.append(consumerThread2)
    consumerThread2.start()
    time.sleep(1)
    producerThread.start()
