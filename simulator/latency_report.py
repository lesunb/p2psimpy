import simpy
import logging
import peer
import network
import driver
import processor
import pandas
from datetime import datetime
from simple_pubsub import *

# Configuração do root logger
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
handlers = [console_handler]
logging.basicConfig(level = logging.INFO,
                    format = '[%(levelname)s] [%(module)15s] %(message)s',
                    handlers = handlers
)

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
        now = self.peer.driver.get_time()
        latency = now - send_time
        self.store_in_latency_list(publisher_id, msg_num, latency)

    def store_in_latency_list(self, publisher_id, msg_num, latency):
        if publisher_id not in self.latency_list:
            self.latency_list[publisher_id] = {}
        self.latency_list[publisher_id][msg_num] = latency

class FileWriterForLatencyReport:

    def __init__(self):
        self.current_time = str(datetime.now()).replace(':', '_')
        self.filename = 'latency_test_' + self.current_time + '.txt'
        self.subscriber_list = None
        self.publisher_count = None
        self.msg_count = None
    
    def get_simulation_data(self, subscriber_list, publisher_count, msg_count):
        self.subscriber_list = subscriber_list
        self.publisher_count = publisher_count
        self.msg_count = msg_count

    def write(self):
        with open(self.filename, 'a') as f:
            f.write('--- Latency Test Report - ' + str(datetime.now()) + '\n\n')
            f.write('Publishers: ' + str(self.publisher_count) + '\n')
            f.write('Subscribers: ' + str(len(self.subscriber_list)) + '\n')
            f.write('Messages per Publisher: ' + str(self.msg_count) + '\n\n')

            for i, subscriber in enumerate(self.subscriber_list):
                f.write('> Subscriber #' + str(i) + '\n')
                for peer_id, msg_dict in subscriber.latency_list.items():
                    f.write('   > Messages from Peer #' + peer_id + ' :\n')
                    for msg_num, latency in msg_dict.items():
                        f.write('       #' + msg_num + ': ' + str(latency) + '\n')
                f.write('\n')
            f.write('\n')

class SimulationDataFrameCreator:

    def __init__(self):
        self.latency_data = []

    def add_single_simulation_data (self, subscriber_list, publisher_number, subscriber_number, msg_num):
        latency_values = []
        for subscriber in subscriber_list:
            for msg_dict in subscriber.latency_list.values():
                for latency in msg_dict.values():
                    latency_values.append(latency)
        latency_series = pandas.Series(data=latency_values)
        lowest_latency = latency_series.min()
        highest_latency = latency_series.max()
        avg_latency = latency_series.mean()
        latency_standard_deviation = latency_series.std()
        simulation_data = (
            publisher_number,
            subscriber_number,
            msg_num,
            avg_latency,
            highest_latency,
            lowest_latency,
            latency_standard_deviation
        )
        self.latency_data.append(simulation_data)
        return
    
    def save_to_csv(self):
        print(len(self.latency_data))
        df = pandas.DataFrame(data=self.latency_data, columns=[
            'Publishers', 'Subscribers', 'Msg Count', 'Avg Latency',
            'Highest Latency', 'Lowest Latency', 'Standard Deviation'])
        #pandas.set_option('display.max_columns', 500)
        time = str(datetime.now()).replace(':', '_')
        df.to_csv('latency_report_' + time + '.csv' , index=False)

class LatencyTest:

    def __init__(self, publisher_number, subscriber_number, network_latency, processor_latency, max_peers, sim_duration):
        # if (publisher_number is 0 or subscriber_number is 0):
        #     raise RuntimeError("Invalid number of peers")
        self.env = simpy.Environment()
        self.net = network.Network(self.env, network_latency, max_peers)
        self.full_connection_time_estimate = max_peers * network_latency
        self.network_latency = network_latency
        self.processor_latency = processor_latency
        self.duration = sim_duration
        self.node_count = 0
        self.topic_name = "latency_test"
        self.msg_quantity = 1
        self.publishers = []
        self.subscribers = []
        self.setup_publishers(publisher_number)
        self.setup_subscribers(subscriber_number)

    def generate_simulation_variables(self):
        pub_range = 100
        sub_range = 100
        # Casos com até 100 publishers e 100 subscribers
        for i in range(pub_range):
            for j in range(sub_range):
                pubs = i + 1
                subs = j + 1
                yield (pubs, subs)

    def reset_simulation(self, publisher_number, subscriber_number, max_peers):
        # if (publisher_number is 0 or subscriber_number is 0):
        #     raise RuntimeError("Invalid number of peers")
        self.env = simpy.Environment()
        self.net = network.Network(self.env, self.network_latency, max_peers)
        self.full_connection_time_estimate = max_peers * self.network_latency
        self.node_count = 0
        self.topic_name = "latency_test"
        self.publishers = []
        self.subscribers = []
        self.setup_publishers(publisher_number)
        self.setup_subscribers(subscriber_number)

    def run_simulations_and_gather_data(self):
        dfcreator = SimulationDataFrameCreator()
        fwriter = FileWriterForLatencyReport()
        for vars in self.generate_simulation_variables():
            publisher_number = vars[0]
            subscriber_number = vars[1]
            max_peers  = publisher_number + subscriber_number
            self.reset_simulation(publisher_number, subscriber_number, max_peers)
            self.env.run()
            self.env.run(until=self.duration)
            fwriter.get_simulation_data(self.subscribers, len(self.publishers), self.msg_quantity)
            fwriter.write()
            dfcreator.add_single_simulation_data(
                self.subscribers, len(self.publishers), len(self.subscribers), self.msg_quantity)
        dfcreator.save_to_csv()

    def setup_simulation_objects(self):
        proc = processor.Processor(self.env, self.node_count, self.processor_latency)
        dri = driver.Driver(self.net, proc)
        new_peer = peer.Peer(dri, self.node_count)
        return (dri, new_peer)
    
    def setup_publishers(self, publisher_number):
        for i in range(publisher_number):
            driver, peer = self.setup_simulation_objects()
            publisher = PublisherForLatencyTest(peer, self.topic_name, self.msg_quantity, self.full_connection_time_estimate)
            self.publishers.append(publisher)
            self.env.process(driver.run())
            self.env.process(publisher.publish())
            self.node_count += 1
    
    def setup_subscribers(self, subscriber_number):
        for i in range(subscriber_number):
            driver, peer = self.setup_simulation_objects()
            subscriber = SubscriberForLatencyTest(peer, self.topic_name, self.full_connection_time_estimate)
            self.subscribers.append(subscriber)
            self.env.process(driver.run())
            self.env.process(subscriber.subscribe())
            self.node_count += 1

    def run(self):
        self.env.run(until=self.duration)
        fwriter = FileWriterForLatencyReport()
        fwriter.get_simulation_data(self.subscribers, len(self.publishers), self.msg_quantity)
        fwriter.write()
        dfcreator = SimulationDataFrameCreator()
        dfcreator.add_single_simulation_data(
            self.subscribers, len(self.publishers), len(self.subscribers), self.msg_quantity)
        dfcreator.save_to_csv()


num_pub = 2
num_sub = 2
max_peers = num_pub + num_sub
net_latency = 2
proc_latency = 3
sim_duration = 1000000

latency_test = LatencyTest(num_pub, num_sub, net_latency, proc_latency, max_peers, sim_duration)
latency_test.run()
#latency_test.run_simulations_and_gather_data()