import threading


class JobThread(threading.Thread):
    def __init__(self, target, args=None, kwargs=None):
        super(JobThread, self).__init__()
        self.target = target
        if not args:
            args = []
        self.args = args
        if not kwargs:
            kwargs = dict()
        self.kwargs = kwargs
        self.result = None

    def run(self):
        self.result = self.target(*self.args, **self.kwargs)

    def get_result(self):
        return self.result
