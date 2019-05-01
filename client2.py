from kafka import KafkaConsumer, KafkaProducer
import threading
from Node import Node
import time
import os
from GrenadeGame import GameSetup, Grenade

X_MAX = 10
Y_MAX = 10


class PlayerThread(threading.Thread):

    def __init__(self, __player, topic=None):
        threading.Thread.__init__(self)
        self.player = __player
        self.topic = topic

    def run(self):
        print('starting thread: %s' % self.topic)
        if self.topic is 'producer':
            self.player.sender()
        else:
            self.player.consumer(self.topic)
        print('starting thread: %s' % self.topic)


class Player:
    grenade_channel = 'grenade_channel'

    def __init__(self, node, __broker):
        self.MyNode = node
        self.broker = __broker
        self.stop = False
        self.threads_list = []
        self.thrown_grenades = []
        self.producer = KafkaProducer(bootstrap_servers=self.broker)

    def set_threads(self):

        self.threads_list.append(PlayerThread(self, 'server_to_'+str(self.MyNode.id)))
        self.threads_list.append(PlayerThread(self, 'grenade'))
        self.threads_list.append(PlayerThread(self, 'producer'))
        for thread in self.threads_list:
            thread.start()

    def printMenu(self):
        print("\n\nEnter integer selection (q to quit)): ")
        print("Update Location 1: ")
        print("Throw Grenade 2: ")
        print("Get Health: 3")
        choice_int = input("Selection: ")
        print("\n\n")
        return choice_int

    def sender(self):

        while self.stop is False:
            choice_int = self.printMenu()
            if choice_int is '1':
                x = input("New X coord = ")
                y = input('New Y coord = ')
                print("Updating Location to (%s,%s)" % (x, y))
                # Update Location HERE
                self.MyNode.x = x
                self.MyNode.y = y
                message = "{} {} {} {}".format(self.MyNode.id, self.MyNode.x, self.MyNode.y, self.MyNode.health).encode('utf8')
                print('\n\nSending update message: %s' % message)
                self.producer.send(self.MyNode.id, message)
            elif choice_int is '2':
                print("Throwing Grenade ... ")
                # Grenade Throw logic HERE
                # Message format: "<type>,<position thrown to>, <from what client_id>"
                direction = input('Enter a direction(0, 90, 180, 270): ')
                velocity = input('Velocity(0-5): ')
                grenade = Grenade(str(self.MyNode.id), str(myNode.x), str(myNode.y), str(velocity),
                                  str(direction), self.producer)
                grenade.grenade_throw()

            elif choice_int is '3':
                print("Your health is = %s" % myNode.get_health())
            else:
                print("Good Bye!")
                self.producer.close()
                self.stop = True
                break
        print('\n\nGame Over')

    # AUTO_OFFSET_RESET_CONFIG = 'earliest' is used if consumers need to look
    # back through the queue
    def consumer(self, topic):
        while self.stop is False:
            consumer = KafkaConsumer(topic, bootstrap_servers=self.broker,)

            for messages in consumer:
                message = messages.value.decode("utf-8")

                if topic == self.grenade_channel:
                    self.handle_grenade(message)
                else:
                    self.handle_update(message)

    def handle_update(self, message):
        msg_type, coords, effect, sender = message.split()

        print('\n\nI received %s from %s' % (msg_type, sender))

        if msg_type == 'grenade_effect':
            if effect == 'no_effect':
                print('\n\ngrenade at %s had no effect' % coords)
            elif effect != self.MyNode.id:
                print('\n\n%s was killed' % effect)
            elif effect == self.MyNode.id:
                print('\n\nSO SORRY %s, you are dead :(' % effect)
                self.stop = True
        elif msg_type == 'health':
            self.MyNode.health = effect
            print('My health is now %s' % self.MyNode.health)

    def handle_grenade(self, message):

        print('\n\nI see grenade %s' % message)

        player_id, x, y, velocity, direction, fuse_length, grenade_id = message.split()
        x = int(x)
        y = int(y)
        velocity = int(velocity)
        fuse_length = round(float(fuse_length))

        if grenade_id in self.thrown_grenades:
            pass
        else:
            self.thrown_grenades.append(grenade_id)

            y_pos = None
            x_pos = None

            if direction == '90' or direction == '270':
                y_pos = y
                if direction == '90':
                    x_pos = x + (velocity * fuse_length)
                else:
                    x_pos = x - (velocity * fuse_length)
                if x_pos > X_MAX:
                    x_pos = X_MAX
                if x_pos < 0:
                    x_pos = 0
            else:
                x_pos = x
                if direction == '0':
                    y_pos = y + (velocity * fuse_length)
                else:
                    y_pos = y - (velocity * fuse_length)
                if y_pos > Y_MAX:
                    y_pos = Y_MAX
                if y_pos < 0:
                    y_pos = 0

            if int(self.MyNode.x) == x_pos and int(self.MyNode.y) == y_pos:

                print('\n\nGrenade exploded at my pos. Waiting for SO SORRY message')
            else:
                print('\n\nGrenade did not explode at my location')


if __name__ == "__main__":

    # client_id = os.environ['CLIENT_ID']
    client_id = 'parker_111'
    broker = 'ec2-3-95-28-49.compute-1.amazonaws.com:9092'
    myNode = Node(client_id, 10, 5, 100)
    GameSetup(broker, myNode)
    player = Player(myNode, broker)

    player.set_threads()


