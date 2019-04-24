from kafka import KafkaConsumer, KafkaProducer


class Node:
    consumer_dict = {}

    def __init__(self, role, broker_addr, client_id, start_position, topic_list, timeout=1000):
        self.client_id = client_id
        self.role = role
        if role == 'client':
            self.grenade = Grenades(start_position)
        self.timeout = timeout
        self.subscribe_topics(topic_list, broker_addr, timeout)
        self.producer = KafkaProducer(bootstrap_servers=broker_addr)

    def grenade_throw(self, velocity=0, angle=0):
        self.grenade.grenade_throw(velocity, angle)

    def subscribe_topics(self, topic_list, broker_addr, timeout=1000):
        for topic in topic_list:
            self.consumer_dict[topic] = KafkaConsumer(topic, bootstrap_servers=broker_addr,
                                                      consumer_timeout_ms=timeout,)

    def publish(self, topic, payload):
        self.producer.send(topic, payload, timestamp_ms=True)


class Grenades:
    health = 100
    client_position = None

    def __init__(self, position):
        self.client_position = position

    def grenade_throw(self, velocity, angle):
        pass

    def grenade_position(self):
        pass

    def position(self):
        return self.client_position

