import traceback


class TaskFailed(Exception):
    pass


class ExceptionCaught:
    def __init__(self, exception):
        self.exception = exception
        self.tb = traceback.format_exc()

    def re_raise(self):
        print(self.tb)
        raise TaskFailed() from self.exception
