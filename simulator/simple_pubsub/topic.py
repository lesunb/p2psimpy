from simple_pubsub import entity
import copy

class Topic(entity.Entity):

    def __init__(self, topic_name, participant):
        super(Topic, self).__init__()
        self.name = topic_name
        self.participant = participant
        self.publishers = {}
        self.remote_subscribers = {} # Handle: IP
        self.local_subscribers = {}
        self.data_objects = {}
        self.creation_time = self.participant.service.driver.get_time()
        self.last_modified = self.creation_time
        self.subscriber_total = 0
        self.local_subscriber_total = 0

    def get_name(self):
        return self.name

    def attach_data_object(self, data_object, current_time):
        if data_object.get_topic_name() == self.name:
            handle = data_object.get_instance_handle()
            self.data_objects[handle] = data_object
            self.last_modified = self.participant.service.driver.get_time()

    def attach_remote_subscriber(self, topic_name, subscriber_handle, network_address):
        if topic_name is self.name and subscriber_handle not in self.remote_subscribers:
            self.remote_subscribers[subscriber_handle] = network_address
            self.subscriber_total += 1

    def attach_local_subscriber(self, topic_name, subscriber):
        subscriber_handle = subscriber.get_instance_handle()
        if topic_name is self.name and subscriber_handle not in self.local_subscribers:
            self.local_subscribers[subscriber_handle] = subscriber
            self.subscriber_total +=1
            self.local_subscriber_total += 1

    def get_subscription_count(self):
        return self.subscriber_total

    def update_subscription_count(self):
        self.subscriber_total = self.local_subscriber_total # Presumimos que somente subscribers locais continuam "vivos".
        sub_dictionary = dict(self.remote_subscribers)
        self.remote_subscribers.clear()
        # O número de subscribers não é atualizado imediatamente, de forma que
        # se lermos o número de subscribers agora, teríamos uma quantidade possivelmente menor que
        # o número real.
        self.participant.service.assert_remote_subscriber_liveliness(self.name, sub_dictionary)
        
