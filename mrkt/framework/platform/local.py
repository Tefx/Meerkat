from ..role import Platform
from ...common.utils import call_on_each


class Hosts(Platform):
    def __init__(self, *services, **options):
        super().__init__(**options)
        self.services = services

    def prepare_services(self, options):
        for service in self.services:
            service.set_options(options, self.options)
        call_on_each(self.services, "prepare_workers")
