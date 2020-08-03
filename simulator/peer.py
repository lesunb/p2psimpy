import logging

"""
Simulates the behavior of a peer in a network.
Uses driver object to interface with the network.

Definir as caracteristicas do nodo basicas - ID , Recursos - Qualidade

Criar um canal broadcast onde o peer precisa se conectar
E criar os links de comunicacao unicast

agente tem (ref de um canal broad)

Resources e uma lista de recursos com a sua qualidade

"""
# -*- coding: utf-8 -*-

class Peer:
    def __init__(self, driver, id):
        self.driver = driver
        self.driver.register_handler(self.on_message)
        self.driver.register_handler(self.on_connect, 'on_connect')
        self.driver.register_handler(self.on_advertise, 'on_advertise')
        self.driver.register_handler(self.on_disconnect, 'on_disconnect')
        self.name = 'peer_{}'.format(id)
        self.latest_read_msg = 0

    def on_message (self, msg):
        logging.info(str(self.driver.env.now) + ' :: ' + '{} received msg: {}'.format(self.name, msg))
        yield None

    def on_connect (self, address):
        logging.info(str(self.driver.env.now) + ' :: ' + '{} connected with address {}'.format(self.name, address))
        for z in self.driver.advertise('Connecting 1, 2, 3'):
            yield z

    def on_disconnect (self):
        self.driver.advertise("Bye World")
    
    def on_advertise (self, msg):
        for z in self.driver.advertise(msg):
            yield z

    def read_new_message(self, subscriber):
        self.latest_read_msg = subscriber.read()