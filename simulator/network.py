# -*- coding: utf-8 -*-
import simpy
import logging

class Network:

    def __init__(self, env, latency, max_hosts = 100):
        self.next_available_address = 1
        self.channel = simpy.Resource(env)
        self.node_list_access = simpy.PriorityResource(env, capacity = 1)   # Para evitar conflito entre send_broadcast e check_lease
        self.env = env
        self.timeout = env.timeout
        self.latency = latency
        self.max_hosts = max_hosts
        self.full_capacity = False
        # DHCP Simples
        self.default_lease_time = 40
        self.addr_list = [{'node':None, 'lease':0} for i in range(self.max_hosts)] # O índice da lista serve como 'IP'
        self.node_list = []                                                 # Usamos esta lista para fazer broadcasts e checar empréstimos

    def register(self, node_driver):
        with self.channel.request() as rec:
            yield rec
            if self.full_capacity:
                logging.warning(str(self.env.now) + ' :: ' + 'Could not register node: Network at full capacity')
                raise ConnectionError("Network at full capacity")
            else:
                curr_address = self.next_available_address
                logging.info(str(self.env.now) + ' :: ' + 'connecting {}'.format(curr_address))
                self.addr_list[curr_address]['node'] = node_driver              # Cria ponteiro para o nodo
                self.addr_list[curr_address]['lease'] = self.env.now + self.default_lease_time
                node_driver.address = curr_address
                node_ptr = {
                    'node': node_driver,
                    'address': curr_address,
                    'time': self.env.now + self.default_lease_time
                }
                self.node_list.append(node_ptr)                                 # Novo empréstimo adicionado
                self.find_next_available()
            yield self.timeout(self.latency)

    def confirm_peer(self, address):
        if address < 0 or address >= self.max_hosts:
            raise RuntimeError(f"Invalid address: {address}")
        elif self.addr_list[address]['node'] is None:
            raise RuntimeError(f"Address not registered: {address}")
        # Substituir por um erro personalizado depois?
        # Talvez expandir verificação para que veja se o endereço realmente corresponde..
        # .. ao driver que enviou mensagem
        # Talvez adicionar mensagem de debug?

    # Adicionar try.. except nas funções de unicast e broadcast, para lidar com erros
    def send_unicast(self, from_addr, to_addr, msg):
        self.confirm_peer(from_addr)
        self.confirm_peer(to_addr)
        logging.info(str(self.env.now) + ' :: ' + 'network sending unicast {} => {}'.format(from_addr, to_addr))
        with self.channel.request() as rec:
            yield rec
            msg_envelope = [from_addr, to_addr, msg]
            node = self.addr_list[to_addr]['node']
            node.receive(msg_envelope)
            yield self.env.timeout(self.latency)

    def send_broadcast(self, from_addr, msg):
        self.confirm_peer(from_addr)
        logging.info(str(self.env.now) + ' :: ' + 'Message Broadcast from {} - {}'.format(from_addr, str(msg)))
        for addr in range(len(self.node_list)):
            to_addr = self.node_list[addr]['address']
            if to_addr == from_addr:
                continue
            msg_envelope = [from_addr, to_addr, msg]
            with self.channel.request() as rec:
                yield rec
                node = self.node_list[addr]['node']
                node.receive(msg_envelope)
                yield self.env.timeout(self.latency)
                #logging.info(str(self.env.now) + ' :: ' + 'Broadcast:'+ str(msg_envelope))

    def find_next_available(self):
        addr = (self.next_available_address + 1) % self.max_hosts
        for __ in range(self.max_hosts - 1):
            if self.addr_list[addr]['node'] == None:
                self.next_available_address = addr
                self.full_capacity = False
                return
            addr = (addr + 1) % self.max_hosts
        self.next_available_address = 0            # Se chegarmos até aqui, então não há endereços disponíveis
        self.full_capacity = True

    def renew(self, address):
        if self.addr_list[address]['node']:
            self.addr_list[address]['lease'] = self.env.now + self.default_lease_time
            logging.info(str(self.env.now) + ' :: ' + 'Lease renewed for address {}'.format(address))
        # Adicionar else

    def check_lease(self):
        with self.node_list_access.request(priority=1) as nl_access:
            yield nl_access
            nodes_to_delete = []
            for i, node in enumerate(self.node_list):
                if node['time'] <= self.env.now:
                    nodes_to_delete.append(i)
            for i, node_number in enumerate(nodes_to_delete):
                self.end_lease(node_number - i)

    def end_lease(self, index):
        address = self.node_list[index]['address']
        self.addr_list[address]['node'] = None
        self.addr_list[address]['lease'] = 0
        del self.node_list[index]
        self.next_available_address = address
        self.full_capacity = False
        logging.info(str(self.env.now) + ' :: ' + 'Lease ended for address {}'.format(address))

    def send_addresses(self, driver):
        addr_list = []
        for peer in self.node_list:
            if peer['address'] is not driver.address:
                addr_list.append(peer['address'])
        return addr_list

    def dhcp(self):
        while True:
            for z in self.check_lease():
                yield z
            yield self.env.timeout(1)