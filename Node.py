from kafka import KafkaConsumer, KafkaProducer
import GrenadeGame


class Node:
    consumer_dict = {}

    def __init__(self, role, broker_addr, client_id, start_position, timeout=1000):
        self.id = client_id
        self.role = role
        if role == 'client':
            self.grenade = GrenadeGame.Grenade()
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


