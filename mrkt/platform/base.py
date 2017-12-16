class BasePlatform:
    def __init__(self, service_cls, service_num):
        self.service_cls = service_cls
        self.service_num = service_num

    def services(self):
        raise NotImplementedError

    def clean(self):
        pass

