import threading
import time
import random


class GrenadeGame:
    send_threads_dict = {}
    receive_threads_dict = {}

    def __init__(self, node, default_game_topic='001', default_connect_topic='players'):
        self.setup = GameSetup(node, default_game_topic, default_connect_topic)
        self.node = node
        self.server_topic = default_game_topic

    def start(self):
        if self.node.role == 'server':
            for player in self.setup.player_list:
                self.send_threads_dict[player] = threading.Thread(target=self.node.producer)
                self.receive_threads_dict[player] = threading.Thread(target=self.node.subscribe_topics('player'+player))
        else:
            self.send_threads_dict['server'] = threading.Thread(target=self.node.producer)
            self.receive_threads_dict['server'] = threading.Thread(target=self.node.subscribe_topics('player' +
                                                                                                     self.node.id))
        self.receive_threads_dict['grenades'] = threading.Thread(target=self.node.subscribe_topics('grenade'))


class GameSetup:
    player_list = []

    def __init__(self, node, game_topic, connect_topic):
        self.game_topic = game_topic
        self.connect_topic = connect_topic
        if node.role == 'client':
            self.client_setup(node)
        else:
            self.server_setup(node)

    def client_setup(self, client):
        print('client publishing %s id to %s' % (client.id, self.connect_topic))

        client.publisher(self.connect_topic, client.id)

    def server_setup(self, server):

        print('Server %s Connecting to players' % server.id)
        find_players = server.subscribe_topics(self.connect_topic, earliest=True)

        for message in find_players:
            message = message.value.decode("utf-8")
            self.player_list.append(message)
            print('connected to %s' % message)


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
