class BasePlatform:
    def __init__(self, service_cls):
        self.service_cls = service_cls

    def services(self):
        raise NotImplementedError

    def clean(self):
        pass

