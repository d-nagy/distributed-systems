import sys


class SignalHandler:
    stopper = None
    worker = None

    def __init__(self, stopper, worker):
        self.stopper = stopper
        self.worker = worker

    def __call__(self, signum, frame):
        print('Handler called.')
        self.stopper.set()
        self.worker.join()

        print('Exiting.')

        sys.exit(0)