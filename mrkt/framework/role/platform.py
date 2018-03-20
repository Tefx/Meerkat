from ...common import call_on_each


class Platform:
    def prepare_services(self, options):
        raise NotImplementedError

    def __init__(self, **options):
        self.services = []
        self.options = options

    def clean(self):
        call_on_each(self.services, "clean", join=True)
