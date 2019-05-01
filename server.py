import time

from GrenadeGame import GameSetup
import threading
from kafka import KafkaConsumer, KafkaProducer

X_MAX = 10
Y_MAX = 10


class ServerThreads(threading.Thread):

    def __init__(self, topic, __server):
        threading.Thread.__init__(self)
        self.topic = topic
        self.server = __server

    def run(self):
        print('starting thread: %s' % self.topic)
        server.consumer_threads(self.topic)


class GrenadeServer:
    grenade_channel = 'grenade_channel'
    broker_addr = 'ec2-3-95-28-49.compute-1.amazonaws.com:9092'
    game = GameSetup(broker_addr)
    player_dict = game.node_dict
    recv_threads_dict = {}
    producer = KafkaProducer(bootstrap_servers=broker_addr)
    thrown_grenades = []

    def set_server_threads(self):
        for player in self.player_dict.keys():
            self.recv_threads_dict[player] = ServerThreads(player, self)
        self.recv_threads_dict['grenade'] = ServerThreads(self.grenade_channel, self)
        for thread in self.recv_threads_dict.values():

            thread.start()
            time.sleep(1)

    def consumer_threads(self, topic):

        consumer = KafkaConsumer(topic, bootstrap_servers=self.broker_addr,)
        for message in consumer:
            message = message.value.decode("utf-8")
            if topic == self.grenade_channel:
                self.handle_grenade(message)
            else:
                self.handle_update(message)

    def handle_grenade(self, message):

        print('\n\nI see grenade %s' % message)

        player_id, x, y, velocity, direction, fuse_length, grenade_id = message.split()
        x = int(x)
        y = int(y)
        velocity = int(velocity)
        fuse_length = round(float(fuse_length))

        if int(self.player_dict[player_id].health) > 0:
            if grenade_id in self.thrown_grenades:
                print('\n\nDuplicate grenade id: %s' % grenade_id)
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

                players_killed = []
                coords = ('(%d,%d)' % (x_pos, y_pos))

                for player in self.player_dict:
                    if int(self.player_dict[player].x) == x_pos and int(self.player_dict[player].y) == y_pos:
                        players_killed.append(player)

                if len(players_killed) == 0:
                    for player in self.player_dict:
                        print('Grenade at %s had no effect sent to %s' % (coords, player))
                        message = '{} {} {} {}'.format('grenade_effect', coords, 'no_effect', 'server').encode('utf8')
                        self.producer.send('server_to_' + player, message)
                else:
                    for player in self.player_dict:
                        for killed in players_killed:
                            print('Grenade at %s killed %s sent to %s' % (coords, killed, player))
                            message = '{} {} {} {}'.format('grenade_effect', coords, killed, 'server').encode('utf8')
                            self.producer.send('server_to_' + player, message)
                    for killed in players_killed:
                        message = '{} {} {} {}'.format('health', coords, '0', 'server').encode('utf8')
                        self.producer.send('server_to_' + killed, message)

    def handle_update(self, message):
        print('\n\nI see update %s' % message)
        player, x, y, health = message.split()

        self.player_dict[player].x = x
        self.player_dict[player].y = y

        print('\nPlayer %s location set to (%s,%s)' % (player, self.player_dict[player].x, self.player_dict[player].y))

        if self.player_dict[player].health != health:
            msg = '{} {} {}'.format('health', self.player_dict[player].health, 'server').encode('utf8')
            self.producer.send('s_to_' + player, msg)


if __name__ == "__main__":
    server = GrenadeServer()
    server.set_server_threads()
