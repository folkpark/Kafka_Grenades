import hashlib
import random
from kafka import KafkaConsumer, KafkaProducer
from collections import defaultdict
from Node import Node


class GameSetup:
    broker_addr = 'ec2-3-95-28-49.compute-1.amazonaws.com:9092'

    def __init__(self, node=None, connect_topic='players'):
        self.connect_topic = connect_topic
        if node is not None:
            self.client_setup(node)
        else:
            self.server_setup()
        self.node_dict = {}

    def client_setup(self, client):
        print('client publishing %s id to %s' % (client.id, self.connect_topic))

        message = "{} {} {} {}".format(client.id, client.health, client.x, client.y).encode('utf8')

        producer = KafkaProducer(bootstrap_servers=self.broker_addr)
        producer.send(self.connect_topic, message)

    def server_setup(self):
        print('Server Connecting to players')
        find_players = KafkaConsumer(self.connect_topic, bootstrap_servers='ec2-3-95-28-49.compute-1.amazonaws.com:9092',
                                     auto_offset_reset='earliest',
                                     consumer_timeout_ms=5000,)

        for message in find_players:
            message = message.value.decode("utf-8")
            player, health, x, y = message.split()
            node = Node(player, x, y, health)
            self.node_dict[player] = node
            print('connected to %s' % message)


class Grenade:
    spoon_depressed: bool

    def __init__(self, player_id, x, y, velocity, direction, producer):
        self.grenade_id = None
        self.fuse_length = Grenade.fuse
        self.player_id = player_id
        self.x = x
        self.y = y
        self.velocity = velocity
        self.direction = direction
        self.producer = producer

    # Direction is cardinal direction in degrees, 0, 90, 180, 270
    # Velocity is spaces per second
    def grenade_throw(self):
        temp = (self.player_id + self.x + self.y + self.velocity + self.direction + str(self.fuse_length)).encode('utf8')
        self.grenade_id = hashlib.md5(temp)

        msg = '{} {} {} {} {} {} {}'.format(self.player_id, self.x, self.y, self.velocity, self.direction,
                                            self.fuse_length, self.grenade_id).encode('utf8')
        self.producer.send('grenade', msg)

    @staticmethod
    def fuse():
        rand = random.random()
        fuse = rand * 5
        if fuse < 3:
            return 3
        else:
            return fuse
