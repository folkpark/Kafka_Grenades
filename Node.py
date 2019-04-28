import time
from kafka import KafkaConsumer, KafkaProducer
import random


class Node:
    consumer_dict = {}

    def __init__(self, role, broker_addr, client_id, start_position, timeout=1000):
        self.id = client_id
        self.role = role
        if role == 'client':
            self.grenade = Grenade()
        self.timeout = timeout
        self.broker_addr = broker_addr
        self.producer = KafkaProducer(bootstrap_servers=self.broker_addr)
        self.position = start_position

    def grenade_throw(self, velocity=0, angle=0):
        self.grenade.grenade_throw(velocity, angle, self.position)

    def subscribe_topics(self, topic):
        self.consumer_dict[topic] = KafkaConsumer(topic, bootstrap_servers=self.broker_addr,
                                                  consumer_timeout_ms=self.timeout,)

    def publish(self, topic, payload):
        self.producer.send(topic, payload, timestamp_ms=True)

    def update_position(self, pos_rep):
        self.position = pos_rep


class Grenade:

    spoon_depressed: bool

    def __init__(self):
        self.pin_inserted = True
        self.spoon_depressed = True
        self.fuse_length = Grenade.fuse()
        self.fuse_time = None

    def pull_pin(self):
        self.pin_inserted = False

    def cook_grenade(self):
        self.spoon_depressed = True
        self.fuse_time = time.time() + self.fuse_length

    def grenade_throw(self, velocity, angle, position):
        pass

    @staticmethod
    def fuse():
        fuse = random.random()*5
        if fuse < 3:
            return 3
        else:
            return fuse
