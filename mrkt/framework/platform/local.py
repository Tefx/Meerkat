from ...common.utils import call_on_each


class BasePlatform:
    def __init__(self, **options):
        self.services = []
        self.options = options

    def prepare_services(self, options):
        raise NotImplementedError

    def clean(self):
        call_on_each(self.services, "clean", join=True)


class Local(BasePlatform):
    def __init__(self, *services, **options):
        super().__init__(**options)
        self.services = services

    def prepare_services(self, options):
        for service in self.services:
            service.set_options(options, self.options)
        call_on_each(self.services, "prepare_workers")
