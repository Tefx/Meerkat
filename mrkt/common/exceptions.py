import traceback


class TaskError(Exception):
    pass


class TaskFailure:
    def __init__(self, exception):
        self.exception = exception
        self.tb = traceback.format_exc()

    def re_raise(self):
        print(self.tb)
        raise TaskError() from self.exception
