from kafka import KafkaConsumer, KafkaProducer
import GrenadeGame


class Node:

    def __init__(self, role, broker_addr, client_id, start_position, timeout=-1):
        self.id = str(client_id)
        self.role = role
        if role == 'client':
            self.grenade = GrenadeGame.Grenade()
        self.timeout = timeout
        self.broker_addr = broker_addr
        self.producer = KafkaProducer(bootstrap_servers=self.broker_addr)
        self.position = start_position

    def grenade_throw(self, velocity=0, angle=0):
        self.grenade.grenade_throw(velocity, angle, self.position)

    def subscribe_topics(self, topic, earliest=False):

        if earliest is True:

            return KafkaConsumer(topic, bootstrap_servers=self.broker_addr,
                                 auto_offset_reset='earliest',
                                 consumer_timeout_ms=self.timeout,
                                 )
        else:
            return KafkaConsumer(topic, bootstrap_servers=self.broker_addr,
                                 consumer_timeout_ms=self.timeout,)

    def publisher(self, topic, payload):
        payload = payload.encode('utf-8')
        self.producer.send(topic, payload)

    def update_position(self, pos_rep):
        self.position = pos_rep


