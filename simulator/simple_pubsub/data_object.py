from simple_pubsub import entity

class Data_Object(entity.Entity):

    def __init__(self, publisher, topic, data):
        super(Data_Object, self).__init__()
        self.publisher = publisher
        self.topic = topic
        self.content = data
        self.creation_time = self.publisher.participant.service.driver.get_time()

    def __str__(self):
        return str(self.content)

    def get_topic_name(self):
        return self.topic.get_name()