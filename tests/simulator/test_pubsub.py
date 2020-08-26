import pytest
import sys, os
myPath = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, myPath + '/../../simulator/')

import simpy
import random
from network import Network
from processor import Processor
from driver import Driver
from peer import Peer
from simple_pubsub import *

@pytest.fixture
def environment_and_network():
    network_latency = 2
    max_peers = 300
    env = simpy.Environment()
    net = Network(env, network_latency, max_peers)
    return (env, net)

@pytest.fixture
def subscriber_number():
    # Set Number:
    return 15

class MsgReceptionTestApp:

    def __init__(self, peer):
        self.peer = peer
        self.latest_read_msg = None

    # Store message
    def callback(self, subscriber):
        self.latest_read_msg = subscriber.read()

class SubscriptionCountTestApp:
    
    def __init__(self, peer, publisher = None):
        self.peer = peer
        self.publisher = publisher

    def subscription_count(self):
        return self.publisher.get_subscriber_count()

def test_simple_publication_two_peers(environment_and_network):
    env, net = environment_and_network
    proc_latency = 3
    random.seed()
    message = random.randrange(1000)
    topic_name = random.randrange(1000)
    wait_before_publication = 100
    wait_before_subscription = 50
    simulation_time = 300
    container = None

    publishing_peer = initialize_peer(env, net, 0, 0, proc_latency)
    subscribing_peer = initialize_peer(env, net, 1, 1, proc_latency)
    app = MsgReceptionTestApp(subscribing_peer)
    publication = wait_then_publish_message(publishing_peer, topic_name, message, wait_before_publication)
    reading = set_up_subscription(app, topic_name, wait_before_subscription)
    add_process_to_simulation(env, publication)
    add_process_to_simulation(env, reading)
    env.run(until=simulation_time)
    container = str(app.latest_read_msg)
    assert container == str(message)

# TODO: Elaborar uma fórmula para determinar automaticamente tempos de espera e de simulação, levando...
# .. em consideração os tempos de latência dados.
def test_simple_publication_to_multiple_peers(environment_and_network, subscriber_number):
    env, net = environment_and_network
    proc_latency = 3
    random.seed()
    message = 'test message'
    topic_name = 'test topic'
    wait_before_publication = 500
    wait_before_subscription = 50
    simulation_time = 3000
    subscriber_id = 1
    subscribers = []
    received_msg = None

    publishing_peer = initialize_peer(env, net, 0, 0, proc_latency)
    publication = wait_then_publish_message(publishing_peer, topic_name, message, wait_before_publication)
    add_process_to_simulation(env, publication)
    for i in range(subscriber_number):
        subscribing_peer = initialize_peer(env, net, 0, i, proc_latency)
        sub_app = MsgReceptionTestApp(subscribing_peer)
        reading = set_up_subscription(sub_app, topic_name, wait_before_subscription)
        add_process_to_simulation(env, reading)
        subscribers.append(sub_app)

    env.run(until=simulation_time)
    for i, subscriber in enumerate(subscribers):
        print(i)
        received_msg = str(subscriber.latest_read_msg)
        assert received_msg == str(message)

def test_get_subscription_count(environment_and_network, subscriber_number):
    env, net = environment_and_network
    proc_latency = 3
    random.seed()
    message = 'test message'
    topic_name = 'test topic'
    wait_before_publication = 500
    wait_before_subscription = 50
    simulation_time = 3000
    subscriber_id = 1
    subscribers = []
    received_msg = None

    publishing_peer = initialize_peer(env, net, 0, 0, proc_latency)
    publisher_app = SubscriptionCountTestApp(publishing_peer)
    publication = set_up_publisher(publisher_app, topic_name, message, wait_before_publication)
    add_process_to_simulation(env, publication)
    for i in range(subscriber_number):
        subscribing_peer = initialize_peer(env, net, 0, i, proc_latency)
        sub_app = MsgReceptionTestApp(subscribing_peer)
        reading = set_up_subscription(sub_app, topic_name, wait_before_subscription)
        add_process_to_simulation(env, reading)
        subscribers.append(sub_app)

    env.run(until=simulation_time)
    sub_count = publisher_app.subscription_count()
    assert sub_count == subscriber_number

# TODO: Adicionar teste mostrando que subscribers não recebem mensagens de tópicos que não
# sejam os seus.

def initialize_peer(environment, network, proc_id, peer_id, proc_latency):
    proc = Processor(environment, proc_id, proc_latency)
    dri = Driver(network, proc)
    peer = Peer(dri, peer_id)
    environment.process(dri.run())
    return peer

def add_process_to_simulation(environment, method):
    environment.process(method)

def set_up_subscription(application, topic_name, wait_time=100):
    yield application.peer.driver.env.timeout(wait_time)
    the_service = pubsub_service.PS_Service(application.peer.driver)
    participant = domain_participant.Domain_Participant(the_service)
    topic = participant.create_topic(topic_name)
    sub = participant.create_subscriber(topic, application.callback)

# TODO: O nome não é adequado: faz mais do que publicar mensagem, antes cria objetos..
# .. necessários. É preciso mudar depois.
def wait_then_publish_message(peer, topic_name, message, wait_time=100):
    yield peer.driver.env.timeout(wait_time)
    the_service = pubsub_service.PS_Service(peer.driver)
    participant = domain_participant.Domain_Participant(the_service)
    topic = participant.create_topic(topic_name)
    pub = participant.create_publisher(topic)
    pub.write(message)

def wait_then_publish_message_multiple_times(peer, topic_name, message, wait_time=100, count=100):
    yield peer.driver.env.timeout(wait_time)
    the_service = pubsub_service.PS_Service(peer.driver)
    participant = domain_participant.Domain_Participant(the_service)
    topic = participant.create_topic(topic_name)
    pub = participant.create_publisher(topic)
    for i in range(count):
        pub.write(message)
        yield peer.driver.env.timeout(1)

def set_up_publisher(application, topic_name, message, wait_time=100):
    yield application.peer.driver.env.timeout(wait_time)
    the_service = pubsub_service.PS_Service(application.peer.driver)
    participant = domain_participant.Domain_Participant(the_service)
    topic = participant.create_topic(topic_name)
    pub = participant.create_publisher(topic)
    application.publisher = pub
    pub.write(message)

def wait_then_read_message(peer, topic_name, message, wait_time=100):
    yield peer.driver.env.timeout(wait_time)
    the_service = pubsub_service.PS_Service(peer.driver)
    participant = domain_participant.Domain_Participant(the_service)
    topic = participant.create_topic(topic_name)
    sub = participant.create_subscriber(topic)
    # Atenção à linha a seguir. Talvez seja necessário alterar o valor mais tarde.
    yield peer.driver.env.timeout(17)  # Tempo para recebimento de mensagens de outros peers contendo dados do domínio.
    peer.latest_read_msg = sub.read()