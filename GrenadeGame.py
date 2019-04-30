import hashlib
import random
from kafka import KafkaConsumer, KafkaProducer
from Node import Node


class GameSetup:

    def __init__(self, broker, node=None, connect_topic='players'):
        self.broker_addr = broker
        self.connect_topic = connect_topic
        self.node_dict = {}
        if node is not None:
            self.client_setup(node)
        else:
            self.server_setup()

    def client_setup(self, client):
        print('client publishing id %s to %s' % (client.id, self.connect_topic))

        message = "{} {} {} {}".format(client.id, client.health, client.x, client.y).encode('utf8')

        producer = KafkaProducer(bootstrap_servers=self.broker_addr)
        producer.send(self.connect_topic, message).get(timeout=2)

    def server_setup(self):
        print('Server Connecting to players')
        find_players = KafkaConsumer(self.connect_topic, bootstrap_servers=self.broker_addr,
                                     consumer_timeout_ms=10000,)

        for message in find_players:
            message = message.value.decode("utf-8")
            player, health, x, y = message.split()
            node = Node(player, x, y, health)
            self.node_dict[player] = node
            print('connected to Player %s with %s health located at (%s,%s)' % (player,health,x,y))


class Grenade:
    spoon_depressed: bool

    def __init__(self, player_id, x, y, velocity, direction, producer,):
        self.grenade_id = None
        self.fuse_length = str(Grenade.fuse)
        self.player_id = str(player_id)
        self.x = str(x)
        self.y = str(y)
        self.velocity = str(velocity)
        self.direction = str(direction)
        self.producer = producer

    # Direction is cardinal direction in degrees, 0, 90, 180, 270
    # Velocity is spaces per second
    def grenade_throw(self, demo=False):

        if demo is True:
            self.velocity = '0'

        temp = (self.player_id + self.x + self.y + self.velocity + self.direction + str(self.fuse_length)).encode('utf8')
        self.grenade_id = hashlib.md5(temp)
        msg = '{} {} {} {} {} {} {}'.format(self.player_id, self.x, self.y, self.velocity, self.direction,
                                            self.fuse_length, self.grenade_id).encode('utf8')
        self.producer.send('grenade', msg).get(timeout=2)
        print('I threw grenade %s' % msg)

    @staticmethod
    def fuse():
        rand = random.random()
        fuse = rand * 5
        if fuse < 3:
            return 3
        else:
            return fuse
