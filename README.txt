Distributed Systems Coursework

Note: This system has been developed and tested primarily on Linux. As such,
      running this on a Linux machine or Mac is preferred. If not possible,
      Windows is also supported.

Dependencies:

    Python 3.6 (or later)
    Pyro4 module



USER INSTRUCTIONS

Basic running of system:

    1. First start the Pyro name server with the following command:

            pyro4-ns

    2. Use the following command to start each replica manager (back end server):

            python replica_manager.py <id> <status>

        <id> (REQUIRED): takes values 0, 1, or 2, corresponding to the data
                            folders replica_0, replica_1 and replica_2 respectively.

        <status> (OPTIONAL):    'active' - set status to active
                                'overloaded' - set status to overloaded
                                'offline' - set status to offline


    3. Start the front end server with:

            python front_end.py

    4. Start the client program with:

            python client.py

    5. Stop each component with CTRL+C.


    To see all program output, start each component (3 replica managers, front
    end and client) in a separate terminal window. This will allow you to view
    all server activity.


Controlling the status of replica managers:

    By default the replica manager will update its status based on probabilities
    every 8 seconds (after each time it gossips). This is the case if it is
    started without a status argument.

    If a status argument is provided, the replica manager will initialise with
    the given status and will not automatically update its own status.

    The status of a replica manager can be set using the command:

            python status_control.py <id> <status>

        <id>:       the ID of the replica manager to set the status of.

        <status>:   'active' - set status to active
                    'overloaded' - set status to overloaded
                    'offline' - set status to offline
                    'manual' - set manual status updating
                    'auto' - set automatic status updating



SYSTEM OVERVIEW

The distributed system consists of 3 replica servers, one front end server and
a client program, implementing the gossip architecture as described in
"Distributed Systems: Concepts and Design" (George Coulouris et. al).

When a client makes a request, the front end selects an appropriate replica
server to forward the request to. It does this by finding all the replica servers
and querying their status. 'Active' servers are preferred, but an 'overloaded'
server will be chosen if there are no 'active' ones. From then on, the front
end will communicate with the same replica manager for the entirety of the user
session unless the replica manager goes 'offline', in which case a new one must
be selected using this scheme. This is done to provide the user with the most
consistent service possible.

The replica servers exchange gossip every 8 seconds, and causal consistency is
provided by using vector timestamps, as discussed in the textbook (Coulouris et. al).
The replica servers maintain logs of the all the updates they have received from
the front end and each other via gossip, and also a log of all the updates they
have executed, so that updates aren't re-executed.

Each component has been implemented so that the failure of one does not cause the
failure of any other.
