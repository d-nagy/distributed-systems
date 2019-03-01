import Pyro4
import random
import uuid
from enum import Enum
from vectorclock import VectorClock
from statusenum import Status


class RType(Enum):
    UPDATE = 0
    QUERY = 1


@Pyro4.expose
class FrontEnd:
    def __init__(self):
        self.servers = self._find_replicas()
        self.ts = VectorClock(len(self.servers))

    def send_request(self, request):
        r_type = self._request_type(request)
        rm = self._choose_replica()

        if r_type == RType.UPDATE:
            rm_ts = rm.send_update(request, self.ts.value(), str(uuid.uuid4()))
            if rm_ts is None:
                return 'Request has already been processed'
            self.ts.merge(VectorClock.fromiterable(rm_ts))
        elif r_type == RType.QUERY:
            val, rm_ts = rm.send_query(request, self.ts.value())
            self.ts.merge(VectorClock.fromiterable(rm_ts))
            return val

    def _choose_replica(self):
        active = [server for server in self.servers
                  if server.get_status() == Status.ACTIVE.value]
        return random.choice(active)

    @staticmethod
    def _request_type(request):
        op = request[0]
        op_command = op.split('_')[0]

        if op_command in ['add', 'del', 'mod']:
            return RType.UPDATE

        if op_command in ['get', 'search']:
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
                "no servers found! (have you started the movie servers first?)"
            )
        return servers


if __name__ == '__main__':
    fe = FrontEnd()

    with Pyro4.Daemon() as daemon:
        uri = daemon.register(fe)
        with Pyro4.locateNS() as ns:
            ns.register('network.frontend', uri)

        print('Front end ready.')

        daemon.requestLoop()
