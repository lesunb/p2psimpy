import simpy
import logging
import peer
import network
import driver
import processor
from simple_pubsub import *

def publisher(peer, topic_name, count):
    environment = peer.driver.env
    yield environment.timeout(50)
    service = pubsub_service.PS_Service(peer.driver)
    node = domain_participant.Domain_Participant(service)
    topic = node.create_topic(topic_name)
    pub = node.create_publisher(topic)
    for i in range(count):
        pub.write(str(topic_name) + ": Message #" + str(i+1))
        yield environment.timeout(2)

def subscriber(peer, topic_name):
    environment = peer.driver.env
    service = pubsub_service.PS_Service(peer.driver)
    node = domain_participant.Domain_Participant(service)
    topic = node.create_topic(topic_name)
    sub = node.create_subscriber(topic, sub_callback)
    yield environment.timeout(0)

def sub_callback(subscriber):
    msg = subscriber.read()
    print("Received: " + str(msg))




# Configuração do root logger
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
handlers = [console_handler]
logging.basicConfig(level = logging.INFO,
                    format = '[%(levelname)s] [%(module)10s] %(message)s',
                    handlers = handlers
)

NUM_PEERS = 2
SIM_DURATION = 2000

# create env
env = simpy.Environment()

# network
net = network.Network(env,2)

#create peers

nodes = []

topic_name = 'demo'

# Setting up publisher
proc_0 = processor.Processor(env, 0, 3)
dri_0 = driver.Driver(net, proc_0)
peer_0 = peer.Peer(dri_0, 0)
env.process(dri_0.run())
env.process(publisher(peer_0, topic_name, 100))

# Setting up subscriber

proc_1 = processor.Processor(env, 1, 3)
dri_1 = driver.Driver(net, proc_1)
peer_1 = peer.Peer(dri_1, 1)
env.process(dri_1.run())
env.process(subscriber(peer_1, topic_name))

# Setting up publisher with no subscribers

proc_2 = processor.Processor(env, 2, 3)
dri_2 = driver.Driver(net, proc_2)
peer_2 = peer.Peer(dri_2, 2)
env.process(dri_2.run())
env.process(publisher(peer_2, 'nosub', 100))

env.run(until=SIM_DURATION)
