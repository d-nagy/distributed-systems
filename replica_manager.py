import csv
import os
import queue
import random
import shutil
import signal
import threading
import time
import Pyro4
from sys import path, argv, platform
from tempfile import NamedTemporaryFile


REPLICA_NUM = 3     # Number of replicas in system

# Variables for data files
movie_file = 'movies.csv'
rating_file = 'ratings.csv'
tag_file = 'tags.csv'

movies_fields = ['movieId', 'title', 'genres']
ratings_fields = ['userId', 'movieId', 'rating', 'timestamp']
tags_fields = ['userId', 'movieId', 'tag', 'timestamp']


def submit_rating(userId, title, rating):
    '''
    Submit a movie rating, overwriting an existing rating if one exists
    for the given movie by the given user.

    Params:
        (int) userId:   the id of the user submitting the rating
        (string) title: title of the movie to submit rating for
        (float) rating: value of the rating (0 - 5)

    '''
    movieId = get_movie_by_title(title)['movieId']
    existing_rating = get_movie_ratings(userId, None, movieId)

    if existing_rating:
        tempfile = NamedTemporaryFile(mode='w', delete=False)
        with open(rating_file, 'r', newline='') as csvfile, tempfile:
            reader = csv.DictReader(csvfile)
            writer = csv.DictWriter(tempfile, ratings_fields)
            writer.writeheader()
            for row in reader:
                if (row['movieId'] == existing_rating[0]['movieId'] and
                        row['userId'] == existing_rating[0]['userId']):
                    new_row = row
                    new_row['rating'] = rating
                    writer.writerow(new_row)
                else:
                    writer.writerow(row)
        shutil.move(tempfile.name, rating_file)
    else:
        with open(rating_file, 'a', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, ratings_fields)
            timestamp = str(int(time.time()))
            writer.writerow({'userId': userId, 'movieId': movieId,
                             'rating': rating, 'timestamp': timestamp})


def submit_tag(userId, title, tag):
    '''
    Submit a tag for a movie.

    Params:
        (int) userId:   the id of the user submitting the rating
        (string) title: title of the movie to submit tag for
        (string) tag:   word to tag the movie with

    '''
    movieId = get_movie_by_title(title)['movieId']
    with open(tag_file, 'a', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, tags_fields)

        writer.writerow({'userId': userId, 'movieId': movieId,
                         'tag': tag, 'timestamp': str(int(time.time()))})


def get_avg_movie_rating(title):
    '''
    Get the average rating for a movie.

    Params:
        (string) title: title of the movie to get the average rating for

    Returns:
        movie_rating: average rating for the given movie
    '''
    movieId = get_movie_by_title(title)['movieId']
    ratings = []
    with open(rating_file, newline='') as csvfile:
        reader = csv.DictReader(csvfile)

        ratings = [float(row['rating']) for row in reader
                   if row['movieId'] == movieId]

    movie_rating = sum(ratings) / len(ratings)
    return movie_rating


def get_movie_ratings(userId=None, title=None, movieId=None):
    '''
    Get movie ratings. If no arguments given, return all movie ratings.

    Params:
        (int) userId:   id of the user whose ratings to get
        (string) title: title of the movie to get the ratings for
        (int) movieId:  id of the movie to get the ratings for

    Returns:
        ratings: list of movie ratings for filter criteria provided
    '''
    ratings = []
    with open(rating_file, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        ratings = [dict(row) for row in reader]
        if userId:
            ratings = [row for row in ratings
                       if int(row['userId']) == userId]
        if movieId:
            ratings = [row for row in ratings
                       if row['movieId'] == movieId]
        elif title:
            movieId = get_movie_by_title(title)['movieId']
            ratings = [row for row in ratings
                       if row['movieId'] == movieId]

    for r in ratings:
        r['title'] = get_movie_by_id(int(r['movieId']))['title']

    return ratings


def get_movie_genres(title):
    '''
    Get all the genres for a movie.

    Params:
        (string) title: title of movie to get genres for

    Returns:
        genres: list of genres for the movie
    '''
    movie = get_movie_by_title(title)
    genres = movie['genres'].split('|')
    return genres


def get_movie_by_title(title):
    '''
    Get a movie based on the title.

    Params:
        (string) title: title of movie to get

    Returns:
        movie: dictionary representing line from the movie CSV file
    '''
    movie = []
    with open(movie_file, newline='') as csvfile:
        reader = csv.DictReader(csvfile)

        movie = next(reader, None)
        while movie is not None:
            if movie['title'].lower()[:-7] != title.lower():
                movie = next(reader, None)
            else:
                break

    if not movie:
        raise Exception(f'No movie found for title [ {title} ].')

    return dict(movie)


def get_movie_by_id(movieId):
    '''
    Get a movie based on the movieId.

    Params:
        (string) movieId: ID of movie to get

    Returns:
        movie: dictionary representing line from the movie CSV file
    '''
    movie = []
    with open(movie_file, newline='') as csvfile:
        reader = csv.DictReader(csvfile)

        movie = next(reader, None)
        while movie is not None:
            if int(movie['movieId']) != movieId:
                movie = next(reader, None)
            else:
                break

    return dict(movie)


def get_movie_tags(title):
    '''
    Get all the tags for a movie.

    Params:
        (string) title: title of movie to get tags for

    Returns:
        genres: list of tags for the movie
    '''
    movieId = get_movie_by_title(title)['movieId']
    tags = []
    with open(tag_file, newline='') as csvfile:
        reader = csv.DictReader(csvfile)

        tags = list({row['tag'] for row in reader
                     if row['movieId'] == movieId})

    return tags


def search_by_title(title):
    '''
    Search movies by title.

    Params:
        (string) title: title to match against

    Returns:
        movies: list of movies whose titles contain title as a substring
    '''
    title = title.lower()
    movies = []
    with open(movie_file, newline='') as csvfile:
        reader = csv.DictReader(csvfile)

        movies = [dict(row) for row in reader if title in row['title'].lower()]

    return movies


def search_by_genre(genre):
    '''
    Search movies by genre.

    Params:
        (string) genre: genre to match against

    Returns:
        movies: list of movies belonging to the given genre
    '''
    genre = genre.lower()
    movies = []
    with open(movie_file, newline='') as csvfile:
        reader = csv.DictReader(csvfile)

        movies = [dict(row) for row in reader
                  if genre in row['genres'].lower()]

    return movies


def search_by_tag(tag):
    '''
    Search movies by tag.

    Params:
        (string) tag: tag to match against

    Returns:
        movies: list of movies that have been tagged with tag
    '''
    tag = tag.lower()
    movies = []
    with open(tag_file, newline='') as csvfile:
        reader = csv.DictReader(csvfile)

        movies = [row['movieId'] for row in reader
                  if tag in row['tag'].lower()]

    with open(movie_file, newline='') as csvfile:
        reader = csv.DictReader(csvfile)

        movies = [dict(row) for row in reader
                  if row['movieId'] in movies]

    return movies


@Pyro4.expose
class ReplicaManager(threading.Thread):
    '''
    Class for a Replica Server within the distributed system, implementing
    the gossip architecture.
    '''

    def __init__(self, replica_id, stopper, status=None):
        super().__init__()

        self._id = replica_id

        # Replica status properties
        self.failure_prob = 0.1
        self.overload_prob = 0.2
        self.auto_status = True
        if status not in [n.value for n in list(Status)]:
            print('Invalid status provided, defaulting to active.')
            self.status = Status.ACTIVE
        else:
            self.status = Status(status)
            self.auto_status = False
            print(f'Status set to {status}.',
                  'Automatic status updating disabled.')

        # Gossip Architecture State
        self.value_ts = VectorClock(REPLICA_NUM)  # aka data timestamp
        self.replica_ts = VectorClock(REPLICA_NUM)  # aka log timestamp
        self.update_log = []
        self.ts_table = [VectorClock(REPLICA_NUM) if i != self._id else None
                         for i in range(REPLICA_NUM)]
        self.executed = []
        self.pending_queries = queue.Queue()
        self.query_results = {}
        self.interval = 8.0  # interval between gossip exchanges
        self.other_replicas = self._find_replicas()

        self.stopper = stopper  # Used to indicate to server to stop

        # Locks for objects shared between threads
        self.vts_lock = threading.Lock()    # for value_ts
        self.rts_lock = threading.Lock()    # for replica_ts
        self.log_lock = threading.Lock()    # for update_log

    def run(self):
        '''
        Override of threading.Thread run() method. Sends gossip to other
        replica managers periodically.
        '''

        while not self.stopper.is_set():
            if self.status != Status.OFFLINE:
                for r_id, rm in self.other_replicas:
                    rm._pyroRelease()
                self.other_replicas = self._find_replicas()

                with self.rts_lock:
                    print('\n--- SENDING GOSSIP ---')
                    for r_id, rm in self.other_replicas:
                        r_ts = self.ts_table[r_id]
                        m_log = self._get_recent_updates(r_ts)

                        print(f'Updates to send to RM {r_id}: ', m_log)

                        try:
                            rm.send_gossip(m_log,
                                           self.replica_ts.value(),
                                           self._id)
                            print(f'Gossip sent to RM {r_id}')
                        except Pyro4.errors.CommunicationError as e:
                            print(f'Failed to send gossip to RM {r_id}')
                    print('----------------------')

            if self.auto_status:
                self._update_status()
            print('Status: ', self.status.value, '\n')
            self.stopper.wait(self.interval)

        print('Stopper set, gossip thread stopping.')

    def send_query(self, q_op, q_prev):
        '''
        Method invoked by the front end to send a query.

        Params:
            (string) q_op:  query command
            (tuple) q_prev: vector timestamp of front end

        Returns:
            response: results of query
        '''

        print('Query received: ', q_op, q_prev)
        response = None

        q_prev = VectorClock.fromiterable(q_prev)

        # stable = are we up to date enough to handle the query correctly?
        stable = False

        with self.vts_lock:
            if q_prev <= self.value_ts:  # stability criteria for query
                val = self._apply_query(q_op)
                new = self.value_ts.value()
                response = (val, new)
                stable = True
                print('Value timestamp: ', self.value_ts.value(), '\n')

        if not stable:
            # if not stable, add to a dictionary of pending queries and wait
            self.query_results[(q_op, q_prev.value())] = queue.Queue(maxsize=1)
            self.pending_queries.put((q_op, q_prev))

            # Wait for query to be executed after some gossip exchange
            response = self.query_results[(q_op, q_prev.value())].get()

            # Remove entry from pending query dictionary
            del self.query_results[(q_op, q_prev.value())]

        return response

    def send_update(self, u_op, u_prev, u_id):
        '''
        Method invoked by the front end to send an update.

        Params:
            (string) u_op:  update command
            (tuple) u_prev: vector timestamp of front end
            (string) u_id:  unique ID for update

        Returns:
            ts: timestamp representing having executed the update or None
                if the update has already been executed
        '''
        print('Update received: ', u_op, u_prev, u_id)
        ts = None

        # Add update to log if it hasn't already been executed
        if u_id not in self.executed:
            with self.rts_lock:
                self.replica_ts.increment(self._id)
                ts = list(u_prev[:])
                ts[self._id] = self.replica_ts.value()[self._id]
                print('Replica timestamp: ', self.replica_ts, '\n')

            ts = VectorClock.fromiterable(ts)

            u_prev = VectorClock.fromiterable(u_prev)
            log_record = (self._id, ts, u_op, u_prev, u_id)
            with self.log_lock:
                self.update_log.append(log_record)
            print('Update record: ', log_record)

            # Execute update if it is stable
            with self.vts_lock:
                if u_prev <= self.value_ts:  # stability criteria for query
                    self._execute_update(u_op, u_id, ts)

            return ts.value()

        return ts

    @Pyro4.oneway
    def send_gossip(self, m_log, m_ts, r_id):
        '''
        Method invoked by other replica managers to send gossip.

        Params:
            (string) m_log: recent updates from replica manager
            (tuple) m_ts:   log timestamp of sending replica manager
            (string) r_id:  ID of sending replica manager

        Returns:
            ts: timestamp representing having executed the update or None
                if the update has already been executed
        '''

        if self.status != Status.OFFLINE:
            print('\n--- RECEIVING GOSSIP ---')
            print(f'Gossip received from RM {r_id}')
            print(m_ts)
            print(m_log)
            print()

            # Merge m_log into update log
            self._merge_update_log(m_log)

            # Merge our replica timestamp with m_ts
            m_ts = VectorClock.fromiterable(m_ts)
            with self.rts_lock:
                self.replica_ts.merge(m_ts)
                print('Replica timestamp: ', self.replica_ts)

            # Execute all updates that have now become stable
            stable = self._get_stable_updates()
            for update in stable:
                _id, ts, u_op, u_prev, u_id = update
                with self.vts_lock:
                    self._execute_update(u_op, u_id, ts)

            # Set the timestamp of the sending replica manager in our timestamp
            # table
            self.ts_table[r_id] = m_ts

            # Execute all stable pending queries
            while True:
                try:
                    q_op, q_prev = self.pending_queries.get(block=False)

                    with self.vts_lock:
                        if q_prev <= self.value_ts:
                            val = self._apply_query(q_op)
                            new = self.value_ts.value()
                            self.query_results[(q_op, q_prev.value())].put(
                                (val, new))

                except queue.Empty:
                    break

            print('------------------------')

    def get_status(self):
        '''
        Method invoked by front end to query the server status.

        Returns:
            status of the server
        '''

        return self.status.value

    def set_status(self, status):
        '''
        Method invoked by status_control.py to set the server status.
        '''

        self.status = Status(status)

    def toggle_auto_status(self, auto):
        '''
        Method invoked by status_control.py to set the server status to
        update automatically or not.
        '''

        if auto:
            self.auto_status = True
        else:
            self.auto_status = False

    def _update_status(self):
        '''
        Set the server status probabilistically.
        '''

        overloaded = random.random()
        failed = random.random()

        if failed < self.failure_prob:
            self.status = Status.OFFLINE
        elif overloaded < self.overload_prob:
            self.status = Status.OVERLOADED
        else:
            self.status = Status.ACTIVE

    def _apply_query(self, q_op):
        '''
        Execute a query command.

        Params:
            (string) q_op: query command to execute

        Returns:
            val: result of query
        '''

        print('Query applied. ', q_op, '\n')
        val = None

        op, *params = q_op
        query = self._parse_q_op(op)
        val = query(*params)

        return val

    def _apply_update(self, u_op):
        '''
        Execute an update command.

        Params:
            (string) u_op: update command to execute
        '''

        print('Update applied.', u_op, '\n')

        op, *params = u_op
        update = self._parse_u_op(op)
        update(*params)

    def _execute_update(self, u_op, u_id, ts):
        '''
        Execute an update.

        Params:
            (string) u_op: update command to execute
            (string) u_id: ID of update to execute
            (VectorClock) ts: timestamp of update to execute
        '''

        # Return immediately if update has already been executed
        if u_id in self.executed:
            return

        self._apply_update(u_op)  # Execute the update
        self.value_ts.merge(ts)  # Update the value timestamp
        self.executed.append(u_id)  # Add update to executed updates
        print('Value timestamp: ', self.value_ts)

    def _merge_update_log(self, m_log):
        '''
        Merge the update log with updates from a gossip message.

        Params:
            m_log: list of updates from a gossip message
        '''

        for record in m_log:
            _id, ts, u_op, u_prev, u_id = record
            ts = VectorClock.fromiterable(ts)
            u_prev = VectorClock.fromiterable(u_prev)
            with self.rts_lock, self.log_lock:
                new_record = (_id, ts, u_op, u_prev, u_id)
                if new_record not in self.update_log:
                    if not ts <= self.replica_ts:
                        self.update_log.append(new_record)

    def _get_stable_updates(self):
        '''
        Retrieve all stable updates from the update log.

        Returns:
            stable: list of updates that can be executed.
        '''

        stable = []

        with self.vts_lock, self.log_lock:
            stable = [record for record in self.update_log
                      if record[3] <= self.value_ts]

        stable.sort(key=lambda r: r[3])

        return stable

    def _get_recent_updates(self, r_ts):
        '''
        Retrieve updates from update log that are more recent than our recorded
        value of the timestamp of another replica manager.

        Params:
            (VectorClock) r_ts: Timestamp of another replica manager, sent in
                                gossip

        Returns:
            recent: all updates from update log that are more recent than the
                    given timestamp
        '''

        recent = []
        with self.log_lock:
            for record in self.update_log:
                _id, ts, u_op, u_prev, u_id = record
                if ts > r_ts:
                    new_record = (_id, ts.value(), u_op, u_prev.value(), u_id)
                    recent.append(new_record)

        return recent

    def _find_replicas(self):
        '''
        Find all online replica managers.

        Returns:
            servers: list of remote server objects for replica managers
        '''

        servers = []
        try:
            with Pyro4.locateNS() as ns:
                for server, uri in ns.list(prefix="network.replica.").items():
                    server_id = int(server.split('.')[-1])
                    if server_id != self._id:
                        servers.append((server_id, Pyro4.Proxy(uri)))
        except Pyro4.errors.NamingError:
            print('Could not find Pyro nameserver.')
        servers.sort()
        return servers[:REPLICA_NUM]

    @staticmethod
    def _parse_q_op(op):
        '''
        Match query command strings with query functions.

        Params:
            (string) op: query command

        Returns:
            function corresponding to the query command
        '''

        return {
            ROp.GET_AVG_RATING.value: get_avg_movie_rating,
            ROp.GET_RATINGS.value: get_movie_ratings,
            ROp.GET_GENRES.value: get_movie_genres,
            ROp.GET_MOVIE.value: get_movie_by_title,
            ROp.GET_TAGS.value: get_movie_tags,
            ROp.SEARCH_TITLE.value: search_by_title,
            ROp.SEARCH_GENRE.value: search_by_genre,
            ROp.SEARCH_TAG.value: search_by_tag
        }[op]

    @staticmethod
    def _parse_u_op(op):
        '''
        Match update command strings with update functions.

        Params:
            (string) op: update command

        Returns:
            function corresponding to the update command
        '''

        return {
            ROp.ADD_RATING.value: submit_rating,
            ROp.ADD_TAG.value: submit_tag
        }[op]


if __name__ == '__main__':
    ID = None
    NAME = None
    STATUS = None
    REPLICADIR = None

    if len(argv) < 2:
        print('No server ID provided, exiting.')
        exit()

    try:
        ID = int(argv[1])
        NAME = f'network.replica.{ID}'
        REPLICADIR = f'replica_{ID}/'
        STATUS = argv[2]
    except ValueError:
        print('Invalid server ID provided, exiting.')
        exit()
    except IndexError:
        pass

    this_dir = os.path.abspath(__file__)
    os.chdir(f'{os.path.dirname(this_dir)}/{REPLICADIR}')
    path.append(os.path.dirname(path[0]))

    from signalhandler import SignalHandler
    from vectorclock import VectorClock
    from enums import Status, ROp

    stopper = threading.Event()
    daemon = Pyro4.Daemon()

    try:
        rm = ReplicaManager(ID, stopper, STATUS)  # Create replica manager

        # Setup signal handler that will shut down our program gracefully
        handler = SignalHandler(stopper=stopper, rm=rm, daemon=daemon)
        signal.signal(signal.SIGINT, handler)

        # Start the gossip thread of the replica manager
        rm.start()

        if not rm.isAlive():
            print('Gossip thread failed to start!')
        else:
            print('Gossip thread started.')

        # Register replica manager with Pyro daemon and nameserver
        uri = daemon.register(rm)
        with Pyro4.locateNS() as ns:
            ns.register(NAME, uri)

        print('Server ready.')

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
