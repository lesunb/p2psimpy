import simpy
import logging
import peer
import network
import driver
import processor
from simple_pubsub import *

class PublisherForLatencyTest:

    def __init__(self, peer, topic_name, count, delay):
        self.peer = peer
        self.id = peer.id
        self.topic_name = topic_name
        self.count = count
        self.delay = delay + 50

    def publish(self):
        yield self.peer.driver.env.timeout(self.delay)
        for z in self.wait_for_connection():
            yield z
        service = pubsub_service.PS_Service(self.peer.driver)
        participant = domain_participant.Domain_Participant(service)
        topic = participant.create_topic(self.topic_name)
        pub = participant.create_publisher(topic)
        for i in range(self.count):
            time = self.peer.driver.get_time()
            msg = (self.id, i, time)
            pub.write(msg)
            yield self.peer.driver.env.timeout(1)

    def wait_for_connection(self):
        while (self.peer.driver.is_connected() is False):
            yield self.peer.driver.env.timeout(1)

class SubscriberForLatencyTest:

    def __init__(self, peer, topic_name, delay):
        self.peer = peer
        self.id = peer.id
        self.topic_name = topic_name
        self.latency_list = {}
        self.delay = delay

    def subscribe(self):
        yield self.peer.driver.env.timeout(self.delay)
        for z in self.wait_for_connection():
            yield z
        service = pubsub_service.PS_Service(self.peer.driver)
        participant = domain_participant.Domain_Participant(service)
        topic = participant.create_topic(self.topic_name)
        sub = participant.create_subscriber(topic, self.get_latency)

    def wait_for_connection(self):
        while (self.peer.driver.is_connected() is False):
            yield self.peer.driver.env.timeout(1)

    def get_latency(self, subscriber):
        msg = subscriber.read().get_raw_data()
        publisher_id = str(msg[0])
        msg_num = str(msg[1])
        send_time = msg[2]
        time = self.peer.driver.get_time()
        latency = time - send_time
        self.store_in_latency_list(publisher_id, msg_num, latency)

    def store_in_latency_list(self, publisher_id, msg_num, latency):
        if publisher_id not in self.latency_list:
            self.latency_list[publisher_id] = {}
        self.latency_list[publisher_id][msg_num] = latency

class LatencyTest:

    def __init__(self, publisher_number, subscriber_number, network_latency, processor_latency, max_peers, sim_duration):
        # if (publisher_number is 0 or subscriber_number is 0):
        #     raise RuntimeError("Invalid number of peers")
        self.env = simpy.Environment()
        self.net = network.Network(self.env, network_latency, max_peers)
        self.full_connection_time_estimate = max_peers * network_latency
        self.processor_latency = processor_latency
        self.duration = sim_duration
        self.node_count = 0
        self.topic_name = "latency_test"
        self.msg_quantity = 1
        self.publishers = []
        self.subscribers = []
        self.setup_publishers(publisher_number)
        self.setup_subscribers(subscriber_number)

    def setup_simulation_variables(self):
        proc = processor.Processor(self.env, self.node_count, self.processor_latency)
        dri = driver.Driver(self.net, proc)
        new_peer = peer.Peer(dri, self.node_count)
        return (dri, new_peer)
    
    def setup_publishers(self, publisher_number):
        for i in range(publisher_number):
            driver, peer = self.setup_simulation_variables()
            publisher = PublisherForLatencyTest(peer, self.topic_name, self.msg_quantity, self.full_connection_time_estimate)
            self.publishers.append(publisher)
            self.env.process(driver.run())
            self.env.process(publisher.publish())
            self.node_count += 1
    
    def setup_subscribers(self, subscriber_number):
        for i in range(subscriber_number):
            driver, peer = self.setup_simulation_variables()
            subscriber = SubscriberForLatencyTest(peer, self.topic_name, self.full_connection_time_estimate)
            self.subscribers.append(subscriber)
            self.env.process(driver.run())
            self.env.process(subscriber.subscribe())
            self.node_count += 1

    def run(self):
        self.env.run(until=self.duration)
        print(self.subscribers[0].latency_list['0']['0'])


# Configuração do root logger
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
handlers = [console_handler]
logging.basicConfig(level = logging.INFO,
                    format = '[%(levelname)s] [%(module)15s] %(message)s',
                    handlers = handlers
)

num_pub = 5
num_sub = 1
max_peers = num_pub + num_sub
net_latency = 2
proc_latency = 3
sim_duration = 10000

latency_test = LatencyTest(num_pub, num_sub, net_latency, proc_latency, max_peers, sim_duration)
latency_test.run()