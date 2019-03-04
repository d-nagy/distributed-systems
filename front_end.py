import Pyro4
import random
import signal
import uuid
from enum import Enum
from vectorclock import VectorClock
from enums import Status, RType
from signalhandler import SignalHandler

REPLICA_NUM = 3


@Pyro4.expose
class FrontEnd:
    def __init__(self):
        self.servers = []
        self.rm = None

        try:
            self.rm = self._choose_replica()
        except ValueError as e:
            print(e)

        self.ts = VectorClock(REPLICA_NUM)

    def send_request(self, request):
        r_type = self._request_type(request)

        try:
            if self.rm is not None:
                rm_status = self.rm.get_status()
                if rm_status == Status.OFFLINE:
                    self.rm = self._choose_replica()
            else:
                self.rm = self._choose_replica()
        except ValueError as e:
            print(e)
            return e.args[0]

        if r_type == RType.UPDATE:
            rm_ts = self.rm.send_update(
                request,
                self.ts.value(),
                str(uuid.uuid4())
            )

            self.ts.merge(VectorClock.fromiterable(rm_ts))

            print('Timestamp: ', self.ts.value())
            return 'Update submitted!'

        elif r_type == RType.QUERY:
            val, rm_ts = self.rm.send_query(request, self.ts.value())

            self.ts.merge(VectorClock.fromiterable(rm_ts))

            print('Timestamp: ', self.ts.value())
            return val

    def _choose_replica(self):
        self.servers = self._find_replicas()

        stat = {server: server.get_status() for server in self.servers}
        available = []
        num_offline = list(stat.values()).count(Status.OFFLINE.value)
        num_overloaded = list(stat.values()).count(Status.OVERLOADED.value)

        if num_offline == len(self.servers):
            raise Exception('All servers offline')
        elif num_overloaded < len(self.servers):
            available = [k for k in stat.keys()
                         if stat[k] == Status.ACTIVE.value]
        else:
            available = list(stat.keys())

        return random.choice(available)

    @staticmethod
    def _request_type(request):
        op = request[0]
        op_type = op.split('.')[0]

        if op_type == 'u':
            return RType.UPDATE

        if op_type == 'q':
            return RType.QUERY

        raise ValueError('command not recognised')

    @staticmethod
    def _find_replicas():
        servers = []
        with Pyro4.locateNS() as ns:
            for server, uri in ns.list(prefix="network.replica.").items():
                print("found replica", server)
                servers.append(Pyro4.Proxy(uri))
        if not servers:
            raise ValueError(
                "No servers found! (are the movie servers running?)"
            )
        return servers[:REPLICA_NUM]


if __name__ == '__main__':
    NAME = 'network.frontend'

    daemon = Pyro4.Daemon()

    try:
        fe = FrontEnd()
        handler = SignalHandler(daemon=daemon)
        signal.signal(signal.SIGINT, handler)

        uri = daemon.register(fe)
        with Pyro4.locateNS() as ns:
            ns.register(NAME, uri)

        print('Front end ready.')

        daemon.requestLoop()

        with Pyro4.locateNS() as ns:
            ns.remove(NAME)

        print('Exiting.')
    except Pyro4.errors.NamingError:
        print('Could not find Pyro nameserver, exiting.')
    finally:
        daemon.close()
