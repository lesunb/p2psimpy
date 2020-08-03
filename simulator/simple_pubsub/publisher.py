from simple_pubsub import entity
from simple_pubsub import data_object

class Publisher(entity.Entity):

    def __init__(self, participant, topic):
        super(Publisher, self).__init__()
        self.participant = participant
        self.topic = topic

    def get_topic(self):
        return self.topic

    def write(self, data):
        new_data = data_object.Data_Object(self, self.topic, data)
        self.participant.service.add_data_object(new_data)