class Entity:

    def __init__(self):
        self.instance_handle = 0

    def set_instance_handle(self, handle):
        if self.instance_handle != 0:
            raise RuntimeError("PS Instance already has a handle.")
        else:
            self.instance_handle = handle

    def get_instance_handle(self):
        if self.instance_handle == 0:
            raise RuntimeError("PS Instance has not been assigned a handle")
        return self.instance_handle