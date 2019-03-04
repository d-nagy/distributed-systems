import sys


class SignalHandler:
    stopper = None
    rm = None
    daemon = None

    def __init__(self, stopper=None, rm=None, daemon=None):
        self.stopper = stopper
        self.rm = rm
        self.daemon = daemon

    def __call__(self, signum, frame):
        print('Handler called.')
        if self.stopper is not None:
            self.stopper.set()

        if self.rm is not None:
            try:
                self.rm.join()
                print('RM stopped.')
            except RuntimeError:
                pass

        if self.daemon is not None:
            print('Shutting down daemon...')
            self.daemon.shutdown()
            print('Daemon stopped.')
