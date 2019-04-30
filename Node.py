# from kafka import KafkaConsumer, KafkaProducer
# import GrenadeGame


class Node:
    # consumer_dict = {}

    def __init__(self, client_id, x, y, health):
        self.id = client_id
        self.x = x
        self.y = y
        self.health = health

    def get_health(self):
        return self.health

    def set_health(self, newHealth):
        self.health = newHealth

    def get_position(self):
        return self.position

    def set_position(self, pos_rep):
        self.position = pos_rep

    def get_id(self):
        return self.id


