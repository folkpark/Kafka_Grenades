import Node
from typing import List
import threading
from kafka import KafkaConsumer, KafkaProducer
import time
import random


class GrenadeGame:

    def __init__(self, client_list: List[Node], server_list: List[Node], default_game_topic='001',
                 default_connect_topic='players'):
        GameSetup(client_list, server_list, default_game_topic, default_connect_topic)


class GameSetup:

    player_list = []

    def __init__(self, client_list, server_list, game_topic, connect_topic):
        self.player_total = len(client_list)
        self.client_setup(client_list)
        self.game_topic = game_topic
        self.connect_topic = connect_topic

    def client_setup(self, client_list):
        for client in client_list:
            client.publish(self.connect_topic, client.id)

    def server_setup(self, server_list):
        player_topics_list = []
        for server in server_list:
            print('Server %s Connecting to %d players' % (server.id, self.player_total))
            find_players = server.subscribe_topics(self.connect_topic)

            for message in find_players:
                self.player_list.append(message.value.decode('utf8'))
            if len(self.player_list) > 1:
                player_topics_list.append()
            else:
                print('%s players are not enough' % len(self.player_list))


class GameThread(threading.Thread):

    def __init__(self, threadID, ):








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
        fuse = random.random() * 5
        if fuse < 3:
            return 3
        else:
            return fuse





