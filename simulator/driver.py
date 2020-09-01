# -*- coding: utf-8 -*-
import random
import simpy
import logging

class Driver:

    def __init__(self, network, processor):
        self.processing_queue = simpy.Store(network.env)
        self.network = network
        self.env = network.env
        self.processor = processor
        self.address = None
        self.methods = {
            'on_message': [],
            'on_connect': [],
            'on_advertise': [],
            'on_disconnect': []
            }
        self.keep_alive_interval = 20
        self.async_calls = simpy.Store(network.env)

    def run(self):
        for z in self.connect():
            yield z
        self.env.process(self.execute_stored_calls())

        while True:
            event = yield self.processing_queue.get()
            for z in self.issue_event(event[0], event[1]):
                if z:
                    yield z
            # for z in self.issue_event(event[0], 'on_advertise'):
            #     if z:
            #         yield z
            
    def connect(self):
        while True:
            try:
                for z in self.network.register(self):
                    yield z
                for z in self.issue_event('on_connect', self.address):
                    yield z
                break     # Se chegarmos aqui, código completado com sucesso, saímos do loop
            except ConnectionError as err:
                print(err.message)
                yield self.env.timeout(1)

    def fetch_peer_list(self):
        return self.network.send_addresses(self)

    def disconnect(self):
        self.address = None

    def is_connected(self):
        return self.address is not None

    def advertise(self, msg):
        for z in self.network.send_broadcast(self.address, msg):
            yield z

    def receive(self, msg_envelope):
        logging.info(str(self.env.now) + ' :: ' + '{} received from {}: {}'.format(
            msg_envelope[1], msg_envelope[0], str(msg_envelope[2])))

        event = ['on_message', msg_envelope]
        self.processing_queue.put(event)

    def send(self, to_addr , msg):
        return self.network.send_unicast(self.address, to_addr, msg)

    def register_handler (self, method, event='on_message'):
        self.methods[event].append(method)

    def issue_event (self, event, value=None):
        logging.info(str(self.env.now) + ' :: ' + 'issuing ' + event)
        for handle in self.methods[event]:
            yield self.env.timeout(1)
            for z in self.processor.process_message(handle, value):
                yield z

    def send_keepalive(self):
        while True:
            yield self.env.timeout(self.keep_alive_interval)
            self.network.renew(self.address)

    def get_time(self):
        return self.env.now

    # Coloca uma função na lista de processamento, que será executada
    # em ordem.
    def async_function_call(self, call_info):
        self.async_calls.put(call_info)

    def execute_stored_calls(self):
        # TODO: Colocar bloco em função separada.
        while True:
            function_call = yield self.async_calls.get()
            function_name = function_call[0]
            # TODO: Mudar implementação para dictionary depois.
            if function_name == 'send':
                to_addr = function_call[1]
                msg = function_call[2]
                self.env.process(self.send(to_addr, msg))
            elif function_name == 'advertise':
                msg = function_call[1]
                self.env.process(self.advertise(msg))