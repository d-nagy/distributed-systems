import csv
import os
import queue
import random
import shutil
import signal
import threading
import time
import Pyro4
from sys import path, argv
from tempfile import NamedTemporaryFile
# from signalhandler import SignalHandler
# from vectorclock import VectorClock
# from enums import Status, ROp


REPLICA_NUM = 3

movie_file = 'movies.csv'
rating_file = 'ratings.csv'
tag_file = 'tags.csv'

movies_fields = ['movieId', 'title', 'genres']
ratings_fields = ['userId', 'movieId', 'rating', 'timestamp']
tags_fields = ['userId', 'movieId', 'tag', 'timestamp']


def submit_rating(userId, title, rating):
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
    movieId = get_movie_by_title(title)['movieId']
    with open(tag_file, 'a', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, tags_fields)

        writer.writerow({'userId': userId, 'movieId': movieId,
                         'tag': tag, 'timestamp': str(int(time.time()))})


def get_avg_movie_rating(title):
    movieId = get_movie_by_title(title)['movieId']
    ratings = []
    with open(rating_file, newline='') as csvfile:
        reader = csv.DictReader(csvfile)

        ratings = [float(row['rating']) for row in reader
                   if row['movieId'] == movieId]

    movie_rating = sum(ratings) / len(ratings)
    return movie_rating


def get_movie_ratings(userId=None, title=None, movieId=None):
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
    movie = get_movie_by_title(title)
    genres = movie['genres'].split('|')
    return genres


def get_movie_by_title(title):
    movie = []
    with open(movie_file, newline='') as csvfile:
        reader = csv.DictReader(csvfile)

        movie = next(reader, None)
        while movie is not None:
            if movie['title'].lower()[:-7] != title.lower():
                movie = next(reader, None)
            else:
                break

    return dict(movie)


def get_movie_by_id(movieId):
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
    movieId = get_movie_by_title(title)['movieId']
    tags = []
    with open(tag_file, newline='') as csvfile:
        reader = csv.DictReader(csvfile)

        tags = list({row['tag'] for row in reader
                     if row['movieId'] == movieId})

    return tags


def search_by_title(title):
    title = title.lower()
    movies = []
    with open(movie_file, newline='') as csvfile:
        reader = csv.DictReader(csvfile)

        movies = [dict(row) for row in reader if title in row['title'].lower()]

    return movies


def search_by_genre(genre):
    genre = genre.lower()
    movies = []
    with open(movie_file, newline='') as csvfile:
        reader = csv.DictReader(csvfile)

        movies = [dict(row) for row in reader
                  if genre in row['genres'].lower()]

    return movies


def search_by_tag(tag):
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
    def __init__(self, replica_id, stopper, status=None):
        super().__init__()

        self._id = replica_id

        # Replica status properties
        self.failure_prob = 0.05
        self.overload_prob = 0.2
        if status not in [n.value for n in list(Status)]:
            self.status = Status.ACTIVE
        else:
            self.status = status

        # Gossip Architecture State
        self.value_ts = VectorClock(REPLICA_NUM)
        self.replica_ts = VectorClock(REPLICA_NUM)
        self.update_log = []
        self.ts_table = [VectorClock(REPLICA_NUM) if i != self._id else None
                         for i in range(REPLICA_NUM - 1)]
        self.executed = []
        self.pending_queries = queue.Queue()
        self.query_results = {}
        self.interval = 5.0
        self.other_replicas = []
        try:
            self.other_replicas = self._find_replicas()
        except ValueError as e:
            print(e)

        self.stopper = stopper
        self.vts_lock = threading.Lock()
        self.rts_lock = threading.Lock()

    def run(self):
        while not self.stopper.is_set():
            if self.status != Status.OFFLINE.value:
                with self.rts_lock:
                    for r_id, rm in self.other_replicas:
                        r_ts = self.ts_table[r_id]
                        m_log = self._get_recent_updates(r_ts)
                        rm.send_gossip(m_log,
                                       self.replica_ts.value(),
                                       self._id)
                        print(f'Gossip sent to RM {r_id}\n')

            self._update_status()
            self.stopper.wait(5)

        print('Stopper set, gossip thread stopping.')

    def send_query(self, q_op, q_prev):
        print('Query received: ', q_op, q_prev)
        response = None

        q_prev = VectorClock.fromiterable(q_prev)

        self.vts_lock.acquire()
        if q_prev <= self.value_ts:
            val = self._apply_query(q_op)
            new = self.value_ts.value()
            response = (val, new)
            self.vts_lock.release()
        else:
            self.vts_lock.release()
            self.query_results[(q_op, q_prev)] = queue.Queue(maxsize=1)
            self.pending_queries.put((q_op, q_prev))
            response = self.query_results[(q_op, q_prev)].get()
            del self.query_results[(q_op, q_prev)]

        print('Timestamp: ', self.value_ts.value())

        return response

    def send_update(self, u_op, u_prev, u_id):
        print('Update received: ', u_op, u_prev, u_id)
        ts = None

        if u_id not in self.executed:
            with self.rts_lock:
                self.replica_ts.increment(self._id)
                ts = list(u_prev[:])
                ts[self._id] = self.replica_ts.value()[self._id]

            ts = VectorClock.fromiterable(ts)

            u_prev = VectorClock.fromiterable(u_prev)
            log_record = (self._id, ts, u_op, u_prev, u_id)
            print('Update record: ' + log_record)

            self.update_log.append(log_record)

            with self.vts_lock:
                if u_prev <= self.value_ts:
                    self._execute_update(u_op, u_id, ts)

            print('Replica timestamp: ', self.replica_ts.value())
            print('Value timestamp: ', self.value_ts.value())

            return ts.value()

        return ts

    @Pyro4.oneway
    def send_gossip(self, m_log, m_ts, r_id):
        print(f'Gossip received from RM {r_id}')
        print(m_ts)
        print(m_log)
        print()
        self._merge_update_log(m_log)

        m_ts = VectorClock.fromiterable(m_ts)
        with self.rts_lock:
            self.replica_ts.merge(m_ts)

        stable = self._get_stable_updates()
        for update in stable:
            _id, ts, u_op, u_prev, u_id = update
            with self.vts_lock:
                self._execute_update(u_op, u_id, ts)

        self.ts_table[r_id] = m_ts

        while True:
            try:
                q_op, q_prev = self.pending_queries.get(block=False)

                with self.vts_lock:
                    if q_prev <= self.value_ts:
                        val = self._apply_query(q_op)
                        new = self.value_ts.value()
                        self.query_results[(q_op, q_prev)].put((val, new))

            except queue.Empty:
                break

    def get_status(self):
        return self.status.value

    def _update_status(self):
        overloaded = random.random()
        failed = random.random()

        if failed < self.failure_prob:
            self.status = Status.OFFLINE
        elif overloaded < self.overload_prob:
            self.status = Status.OVERLOADED
        else:
            self.status = Status.ACTIVE

    def _apply_query(self, q_op):
        print('Query applied.')
        val = None

        op, *params = q_op
        query = self._parse_q_op(op)
        val = query(*params)

        return val

    def _apply_update(self, u_op):
        print('Update applied.')

        op, *params = u_op
        update = self._parse_u_op(op)
        update(*params)

    def _execute_update(self, u_op, u_id, ts):
        if u_id in self.executed:
            return

        self._apply_update(u_op)
        self.value_ts.merge(ts)
        self.executed.append(u_id)

    def _merge_update_log(self, m_log):
        for record in m_log:
            _id, ts, u_op, u_prev, u_id = record
            with self.rts_lock:
                if not ts <= self.replica_ts:
                    self.update_log.append(record)

    def _get_stable_updates(self):
        stable = []

        with self.vts_lock:
            stable = [record for record in self.update_log
                      if record[3] <= self.value_ts]

        stable.sort(key=lambda r: r[3])

        return stable

    def _get_recent_updates(self, r_ts):
        return [record for record in self.update_log if record[3] > r_ts]

    def _find_replicas(self):
        servers = []
        with Pyro4.locateNS() as ns:
            for server, uri in ns.list(prefix="network.replica.").items():
                server_id = int(server.split('.')[-1])
                if server_id != self._id:
                    print("found replica", server)
                    servers.append((server_id, Pyro4.Proxy(uri)))
        if not servers:
            raise ValueError(
                "No other servers found."
            )
        servers.sort()
        return servers[:REPLICA_NUM]

    @staticmethod
    def _parse_q_op(op):
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
        return {
            ROp.ADD_RATING.value: submit_rating,
            ROp.ADD_TAG.value: submit_tag
        }[op]


if __name__ == '__main__':
    ID = None
    STATUS = None
    REPLICADIR = None

    if len(argv) < 2:
        print('No server ID provided, exiting.')

    try:
        ID = int(argv[1])
        REPLICADIR = f'replica_{ID}/'
        STATUS = argv[2]
        print(ID)
    except ValueError:
        print('Invalid server ID provided, exiting.')
    except IndexError:
        pass

    this_dir = os.path.abspath(__file__)
    os.chdir(f'{os.path.dirname(this_dir)}/{REPLICADIR}')
    path.append(os.path.dirname(path[0]))

    from signalhandler import SignalHandler
    from vectorclock import VectorClock
    from enums import Status, ROp

    try:
        stopper = threading.Event()
        rm = ReplicaManager(ID, stopper, STATUS)
        handler = SignalHandler(stopper, rm)
        signal.signal(signal.SIGINT, handler)
        rm.start()

        if not rm.isAlive():
            print('Gossip thread failed to start!')
        else:
            print('Gossip thread started.')

        with Pyro4.Daemon() as daemon:
            uri = daemon.register(rm)
            with Pyro4.locateNS() as ns:
                ns.register(f'network.replica.{ID}', uri)

            print('Server ready.')

            daemon.requestLoop(loopCondition=lambda: not stopper.is_set())
    except Pyro4.errors.NamingError:
        print('Could not find Pyro nameserver, exiting.')
