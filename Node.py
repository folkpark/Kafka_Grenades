# from kafka import KafkaConsumer, KafkaProducer
# import GrenadeGame


class Node:
    # consumer_dict = {}

    def __init__(self, client_id, start_position, health):
        self.id = client_id
        self.position = start_position
        self.health = health
        #self.role = role
        # if role == 'client':
        #     self.grenade = GrenadeGame.Grenade()
        #self.timeout = timeout
        #self.broker_addr = broker_addr
        #self.producer = KafkaProducer(bootstrap_servers=self.broker_addr)

    # def grenade_throw(self, velocity=0, angle=0):
    #     self.grenade.grenade_throw(velocity, angle, self.position)

    # def subscribe_topics(self, topic):
    #     self.consumer_dict[topic] = KafkaConsumer(topic, bootstrap_servers=self.broker_addr,
    #                                               consumer_timeout_ms=self.timeout,)

    # def publish(self, topic, payload):
    #     self.producer.send(topic, payload, timestamp_ms=True)


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


