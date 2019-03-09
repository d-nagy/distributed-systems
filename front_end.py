import Pyro4
import random
import signal
import threading
import uuid
from enum import Enum
from vectorclock import VectorClock
from enums import Status, RType
from signalhandler import SignalHandler
from sys import platform

REPLICA_NUM = 3


@Pyro4.expose
class FrontEnd:
    '''
    Class for Front End Server within the distributed system.
    '''

    def __init__(self):
        self.servers = []
        self.rm = None

        # Initial selection of replica manager to communicate with
        try:
            self.rm = self._choose_replica()
        except ValueError as e:
            print(e)

        self.ts = VectorClock(REPLICA_NUM)  # Vector timestamp of front end

    def send_request(self, request):
        '''
        Method invoked by client to send a request.

        Params:
            (tuple) request: command to execute and arguments for the command

        Returns:
            If the request is a query, return the results of the query,
            otherwise a confirmation message.
        '''

        r_type = self._request_type(request)

        # Find a replica manager to send request to if the original is
        # unavailable
        if self.rm is not None:
            try:
                rm_status = self.rm.get_status()
                print(rm_status)
                if rm_status == Status.OFFLINE.value:
                    self.rm = self._choose_replica()
            except Pyro4.errors.ConnectionClosedError:
                self.rm = self._choose_replica()
        else:
            self.rm = self._choose_replica()

        if r_type == RType.UPDATE:
            rm_ts = self.rm.send_update(
                request,
                self.ts.value(),
                str(uuid.uuid4())
            )

            print('Update sent: ', request)

            self.ts.merge(VectorClock.fromiterable(rm_ts))

            print('Front end timestamp: ', self.ts.value())
            return 'Update submitted!'

        elif r_type == RType.QUERY:
            val, rm_ts = self.rm.send_query(request, self.ts.value())

            print('Query sent: ', request)

            self.ts.merge(VectorClock.fromiterable(rm_ts))

            print('Front end timestamp: ', self.ts.value())
            return val

    def _choose_replica(self):
        '''
        Select a replica manager to communicate with.

        Return:
            Remote object for a replica manager
        '''

        for server in self.servers:
            server._pyroRelease()

        self.servers = self._find_replicas()

        stat = {server: server.get_status() for server in self.servers}
        available = []
        num_offline = list(stat.values()).count(Status.OFFLINE.value)
        num_active = list(stat.values()).count(Status.ACTIVE.value)

        if num_active > 0:
            available = [k for k in stat.keys()
                         if stat[k] == Status.ACTIVE.value]
        elif num_offline == len(self.servers):
            raise Exception('All servers offline')
        else:
            available = [k for k in stat.keys()
                         if stat[k] != Status.OFFLINE.value]

        if not available:
            return None

        return random.choice(available)

    @staticmethod
    def _request_type(request):
        '''
        Determine whether a request is an update or query.

        Params:
            (tuple) request: request to check

        Returns:
            Enum representing the type of request
        '''

        op = request[0]
        op_type = op.split('.')[0]

        if op_type == 'u':
            return RType.UPDATE

        if op_type == 'q':
            return RType.QUERY

        raise ValueError('command not recognised')

    @staticmethod
    def _find_replicas():
        '''
        Find all online replica managers.

        Returns:
            servers: list of remote server objects for replica managers
        '''

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
        fe = FrontEnd()  # Create front end

        # Setup signal handler that will shut down our program gracefully
        handler = SignalHandler(daemon=daemon)
        signal.signal(signal.SIGINT, handler)

        # Register front end with Pyro daemon and nameserver
        uri = daemon.register(fe)
        with Pyro4.locateNS() as ns:
            ns.register(NAME, uri)

        print('Front end ready.')

        # Start listening for remote calls
        if platform == 'win32':
            threading.Thread(target=daemon.requestLoop).start()
        else:
            daemon.requestLoop()

        # Before exiting, unregister replica manager from name server
        with Pyro4.locateNS() as ns:
            ns.remove(NAME)

        print('Exiting.')
    except Pyro4.errors.NamingError:
        print('Could not find Pyro nameserver, exiting.')
    finally:
        daemon.close()
