# TODO
# - Não pedir todos os objetos-dado na criação do PS Service; Fazer isso na..
#   .. criação de subscriber, e somente dados do tópico específico.

import logging
import copy
from threading import Lock
from singleton import Singleton
from simple_pubsub import entity

class UniqueHandleController(metaclass=Singleton):

    def __init__(self):
        self.next_available_handle = 1
        self.lock = Lock()

    def generate_handle(self):
        handle = None
        with self.lock:
            handle = self.next_available_handle
            self.next_available_handle += 1
        return handle

class PS_Service(entity.Entity):

    def __init__(self, driver):
        self.handle_controller = UniqueHandleController()
        self.instance_handle = self.handle_controller.generate_handle()
        self.driver = driver
        self.peer_list = []
        self.local_participants = {} # Handle: Participant
        self.remote_participants = {} # Handle: IP
        self.topics = {}  # Topic name: Topic
        self.data_objects = {} # Handle: Data object
        self.message_handlers = {}

        self._add_message_handler_methods()
        self._attach_msg_reception_handler_to_driver()
        self._discover_peers()
        #self._request_full_domain_data()

    def set_instance_handle(self, handle):
        raise RuntimeError("PS Service's handle cannot be changed.")

    def get_instance_handle(self):
        return self.instance_handle

    def _send_local_modification(self, type_name, data):
        change = (type_name, data)
        self._send_to_all_peers(change)
    
    def _send_to_all_peers(self, msg):
        self.driver.async_function_call(['advertise', msg])

    def assign_handle(self, entity):
        handle = self.handle_controller.generate_handle()
        entity.set_instance_handle(handle)
    
    def add_participant(self, participant):
        self.assign_handle(participant)
        handle = participant.get_instance_handle()
        self.local_participants[handle] = participant
        local_address = self.driver.address
        self._send_local_modification('NEW_PARTICIPANT', (handle, local_address))

    def add_topic(self, topic):
        self.assign_handle(topic)
        topic_key = topic.get_name()
        self.topics[topic_key] = topic
        #self._send_local_modification('NEW_TOPIC', topic)

    def add_data_object(self, data_object):
        self.assign_handle(data_object)
        handle = data_object.get_instance_handle()
        self.data_objects[handle] = data_object
        self._attach_data_object_to_topic(data_object)
        self._send_local_modification('NEW_DATA', data_object)

    def announce_new_subscriber(self, new_subscriver):
        self._send_local_modification('NEW_SUBSCRIBER', new_subscriber)

    def topic_exists(self, topic_name):
        return topic_name in self.topics

    def get_topic(self, topic_name):
        if self.topic_exists(topic_name):
            return self.topics[topic_name]
        else:
            return None
    
    def _erase_topic_from_domain(self, topic):
        # Deleta tópico e todos os dados associados a ele.
        pass

    def _discover_peers(self):
        self.peer_list = self.driver.fetch_peer_list()

    def _add_message_handler_methods(self):
        self.message_handlers['NEW_PARTICIPANT'] = self._append_remote_participant
        # self.message_handlers['NEW_TOPIC'] = self._append_remote_topic
        self.message_handlers['NEW_DATA'] = self._append_data_object
        self.message_handlers['SEND_ALL_DATA'] = self._send_full_domain_data
        self.message_handlers['ALL_DATA'] = self._receive_full_domain_data
        self.message_handlers['NEW_SUBSCRIBER'] = self._acknowledge_new_remote_subscriber
        self.message_handlers['CONFIRM_SUBSCRIBER'] = self.confirm_subscriber_existence

    # Espera uma 2-tupla de handle e IP.
    def _append_remote_participant(self, r_participant_info):
        handle = r_participant_info[0]
        remote_ip = r_participant_info[1]
        if handle not in self.remote_participants:
            self.remote_participants[handle] = remote_ip

    # def _append_remote_topic(self, r_topic):
    #     topic_name = r_topic.get_name()
    #     if not self.topic_exists(topic_name):
    #         handle = r_topic.get_instance_handle()
    #         self.topics[topic_name] = r_topic
    #     else:
    #         self._resolve_topic_conflict(r_topic)

    # def _resolve_topic_conflict(self, topic):
    #     # TODO: Completar este método.
    #     # O tópico com a instance handle menor tem prioridade.
    #     # Caso o serviço local tenha prioridade, é necessário informar os outros nodos.
    #     pass

    def _append_data_object(self, new_data):
        handle = new_data.get_instance_handle()
        self.data_objects[handle] = new_data
        self._send_data_object_to_all_participants(new_data)
        self._attach_data_object_to_topic(new_data)

    def notify_remote_participants_of_new_subscriber(self, subscriber):
        topic_name = subscriber.get_topic().get_name()
        local_address = self.driver.address
        handle = subscriber.get_instance_handle()
        subscriber_info = (topic_name, handle, local_address)
        msg = ('NEW_SUBSCRIBER', subscriber_info)
        self._send_to_all_peers(msg)

    def _acknowledge_new_remote_subscriber(self, new_subscriber_info):
        topic_name = new_subscriber_info[0]
        handle = new_subscriber_info[1]
        address = new_subscriber_info[2]
        if self.topic_exists(topic_name):
            self.topics[topic_name].attach_remote_subscriber(topic_name, handle, address)

    def assert_remote_subscriber_liveliness(self, topic_name, sub_dictionary):
        local_address = self.driver.address
        for handle, to_address in sub_dictionary.items():
            info = (topic_name, handle, local_address)
            msg = ('CONFIRM_SUBSCRIBER', info)
            self.driver.async_function_call(['send', to_address, msg])

    def confirm_subscriber_existence(self, subscriber_info):
        topic_name = subscriber_info[0]
        sub_handle = subscriber_info[1]
        remote_address = subscriber_info[2]
        for participant in self.local_participants.values():
            if sub_handle in participant.subscribers:
                subscriber = participant.subscribers[sub_handle]
                self.confirm_subscription_to_remote_participant(subscriber, remote_address)

    def confirm_subscription_to_remote_participant(self, subscriber, to_address):
        topic_name = subscriber.get_topic().get_name()
        local_address = self.driver.address
        handle = subscriber.get_instance_handle()
        subscriber_info = (topic_name, handle, local_address)
        msg = ('NEW_SUBSCRIBER', subscriber_info)
        self.driver.async_function_call(['send', to_address, msg])

    def _send_data_object_to_all_participants(self, data_object):
        topic_name = data_object.get_topic_name()
        if self.topic_exists(topic_name):
            for participant in self.local_participants.values():
                participant.update_all_subscribers(data_object)

    def _attach_data_object_to_topic(self, data_object):
        topic_name = data_object.get_topic_name()
        if self.topic_exists(topic_name):
            current_time = self.driver.get_time()
            self.topics[topic_name].attach_data_object(data_object, current_time)

    def _send_full_domain_data(self, to_address):
        local_data = []
        for participant in self.local_participants.values():
            local_address = self.driver.address
            packet = ('NEW_PARTICIPANT', (participant.get_instance_handle(), local_address))
            local_data.append(packet)
        # No momento, não queremos enviar todos os dados para todos os nodos.
        #
        # for data_object in self.data_objects.values():
        #     packet = ('NEW_DATA', data_object)
        #     local_data.append(packet)
        msg = ('ALL_DATA', local_data)
        self.driver.async_function_call(['send', to_address, msg])

    def _receive_full_domain_data(self, r_data):
        for element in r_data:
            self._interpret_data(element)

    def _unpack_data(self, msg):
        # Formato esperado da mensagem:
        # [0] Remetente; [1] Destinatário; [2] Mensagem em si
        data = msg[2]
        self._interpret_data(data)
        yield self.driver.env.timeout(0)

    def _interpret_data(self, data):
        # Presumimos que os dados estejam em uma 2-tupla, sendo o primeiro elemento..
        # .. uma string descrevendo o pedido, o segundo elemento os dados em si
        if data[0] not in self.message_handlers:
            pass
            #logging.warning(str(self.driver.get_time()) + ' :: ' + f'PSS Service (Handle {str(self.instance_handle)}): Invalid request: {str(data[1])}')
        else:
            self.message_handlers[data[0]](data[1])

    def _attach_msg_reception_handler_to_driver(self):
        self.driver.register_handler(self.receive_incoming_data, 'on_message')

    def receive_incoming_data(self, msg):
        logging.info(str(self.driver.get_time()) + ' :: ' + f'Data received by PS Service, handle {str(self.instance_handle)}')
        for z in self._unpack_data(msg):
            yield z

    def _request_full_domain_data(self):
        request_msg = ('SEND_ALL_DATA', self.driver.address)
        self._send_to_all_peers(request_msg)

    def retrieve_all_data_objects(self):
        return self.data_objects.values()

    def retrieve_filtered_data_objects(self, topic_name):
        data = []
        for element in self.data_objects.values():
            if element.get_topic_name() == topic_name:
                data.append(element)
        return data