from GrenadeGame import GameSetup
import threading
from kafka import KafkaConsumer, KafkaProducer
from collections import defaultdict

X_MAX = 10
Y_MAX = 10


class GrenadeServer:
    broker_addr = 'ec2-3-95-28-49.compute-1.amazonaws.com:9092'
    game = GameSetup()
    player_dict = game.node_dict
    recv_threads_dict = {}
    producer = KafkaProducer(bootstrap_servers=broker_addr)
    thrown_grenades = []

    def set_server_threads(self):
        for player in self.player_dict.keys():
            self.recv_threads_dict[player] = threading.Thread(target=self.consumer_threads(player))
        for thread in self.recv_threads_dict:
            thread.start()

    def consumer_threads(self, topic):

        consumer = KafkaConsumer(topic, bootstrap_servers=self.broker_addr,)
        for message in consumer:
            message = message.value.decode("utf-8")
            if topic == 'grenade':
                self.handle_grenade(message)
            else:
                self.handle_update(message)

    def handle_grenade(self, message):

        player_id, x, y, velocity, direction, fuse_length, grenade_id = message.split()
        x = int(x)
        y = int(y)
        velocity = int(velocity)
        fuse_length = round(float(fuse_length))

        if self.player_dict[player_id].health > 0:
            if grenade_id in self.thrown_grenades:
                print('Duplicate grenade id: %s' % grenade_id)
            else:
                self.thrown_grenades.append(grenade_id)

                y_pos = None
                x_pos = None

                if direction == '90' or direction == '270':
                    y_pos = y
                    x_pos = x + (velocity*fuse_length)

                    if x_pos > X_MAX:
                        x_pos = X_MAX
                    if x_pos < 0:
                        x_pos = 0
                else:
                    x_pos = x
                    y_pos = y + (velocity*fuse_length)

                    if y_pos > Y_MAX:
                        y_pos = Y_MAX
                    if y_pos < 0:
                        y_pos = 0

                for player in self.player_dict:
                    if self.player_dict[player].x == x_pos and self.player_dict[player].y == y_pos:
                        self.producer.send(player, b'SO SORRY')
                        self.player_dict[player].health = 0

    def handle_update(self, message):

        player_id, x, y, health = message.split()

        self.player_dict[player_id].x = x
        self.player_dict[player_id].y = y

        if self.player_dict[player_id].health != health:
            msg = '{} {}'.format('health', self.player_dict[player_id].health).encode('utf8')
            self.producer.send(player_id, msg)


if __name__ == "__main__":
    GrenadeServer()