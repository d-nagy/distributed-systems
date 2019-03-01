import csv
import os
import queue
import random
import signal
import threading
import time
import Pyro4
from sys import path


movie_file = 'movies.csv'
rating_file = 'ratings.csv'
tag_file = 'tags.csv'

movies_fields = ['movieId', 'title', 'genres']
ratings_fields = ['userId', 'movieId', 'rating', 'timestamp']
tags_fields = ['userId', 'movieId', 'tag', 'timestamp']


def submit_rating(userId, movieId, rating):
    with open(rating_file, 'a', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, ratings_fields)

        writer.writerow({'userId': userId, 'movieId': movieId,
                         'rating': rating, 'timestamp': str(int(time.time()))})


def submit_tag(userId, movieId, tag):
    with open(tag_file, 'a', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, tags_fields)

        writer.writerow({'userId': userId, 'movieId': movieId,
                         'tag': tag, 'timestamp': str(int(time.time()))})


def get_avg_movie_rating(movieId):
    ratings = []
    with open(rating_file, newline='') as csvfile:
        reader = csv.DictReader(csvfile)

        ratings = [float(row['rating']) for row in reader
                   if int(row['movieId']) == movieId]

    movie_rating = sum(ratings) / len(ratings)
    return movie_rating


def get_movie_ratings(userId=None, movieId=None):
    ratings = []
    with open(rating_file, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        ratings = [dict(row) for row in reader]
        if userId:
            ratings = [row for row in ratings
                       if int(row['userId']) == userId]
        if movieId:
            ratings = [row for row in ratings
                       if int(row['movieId']) == movieId]

    return ratings


def get_movie_genres(movieId):
    movie = get_movie(movieId)
    genres = movie['genres'].split('|')
    return genres


def get_movie(movieId):
    movie = None
    with open(movie_file, newline='') as csvfile:
        reader = csv.DictReader(csvfile)

        movie = next(reader, None)
        while movie is not None:
            if int(movie['movieId']) != movieId:
                movie = next(reader, None)
            else:
                break

    return dict(movie)


def get_movie_tags(movieId):
    tags = []
    with open(tag_file, newline='') as csvfile:
        reader = csv.DictReader(csvfile)

        tags = list({row['tag'] for row in reader
                     if int(row['movieId']) == movieId})

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


@Pyro4.expose
class ReplicaManager(threading.Thread):
    def __init__(self, replica_id, stopper):
        super().__init__()

        self._id = replica_id

        # Replica status properties
        self.failure_prob = 0.05
        self.overload_prob = 0.2
        self.status = Status.ACTIVE

        # Gossip Architecture State
        self.value_ts = VectorClock(3)
        self.replica_ts = VectorClock(3)
        self.update_log = []
        self.ts_table = [(0, 0, 0), (0, 0, 0)]
        self.executed = []
        self.pending_queries = queue.Queue()
        self.query_results = {}

        self.stopper = stopper
        self.ts_lock = threading.Lock()

    def run(self):
        while not self.stopper.is_set():
            updates = self.update_log

            # Apply stable updates
            for update in updates:
                _id, ts, u_op, u_prev, u_id = update

                self.ts_lock.acquire()
                stable = u_prev <= self.value_ts
                self.ts_lock.release()

                if stable:
                    self._execute_update(u_op, u_id, ts)

            # Try to apply next pending query
            try:
                q_op, q_prev = self.pending_queries.get(block=False)

                self.ts_lock.acquire()

                if q_prev <= self.value_ts:
                    val = self._apply_query(q_op)
                    new = self.value_ts.value()
                    self.query_results[(q_op, q_prev)].put((val, new))

                self.ts_lock.release()
            except queue.Empty:
                pass

        print('Stopper set.')

    def send_query(self, q_op, q_prev):
        print('Query received: ', q_op, q_prev)
        response = None

        q_prev = VectorClock.fromiterable(q_prev)
        self.query_results[(q_op, q_prev)] = queue.Queue(maxsize=1)
        self.pending_queries.put((q_op, q_prev))
        response = self.query_results[(q_op, q_prev)].get()
        del self.query_results[(q_op, q_prev)]

        return response

    def send_update(self, u_op, u_prev, u_id):
        print('Update received: ', u_op, u_prev, u_id)
        ts = None

        if u_id not in self.executed:
            self.replica_ts.increment(self._id)

            ts = u_prev[:]
            ts[self._id] = self.replica_ts.value()[self._id]
            ts = VectorClock.fromiterable(ts)

            u_prev = VectorClock.fromiterable(u_prev)
            log_record = (self._id, ts, u_op, u_prev, u_id)

            self.update_log.append(log_record)

            return ts.value()

        return ts

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

        self.ts_lock.acquire()
        self.value_ts.merge(ts)
        self.ts_lock.release()

        self.executed.append(u_id)

    def _merge_update_log(self, m_log):
        for record in m_log:
            _id, ts, u_op, u_prev, u_id = record
            if not ts <= self.replica_ts:
                self.update_log.append(record)

    def _get_stable_updates(self):
        self.ts_lock.acquire()
        stable = [record for record in self.update_log
                  if record[3] <= self.value_ts]
        self.ts_lock.release()

        stable.sort(key=lambda r: r[3])

        return stable

    @staticmethod
    def _parse_q_op(op):
        return {
            'get_ratings': get_movie_ratings,
            'get_genres': get_movie_genres,
            'get_movie': get_movie,
            'get_tags': get_movie_tags,
            'search_title': search_by_title,
            'search_genre': search_by_genre
        }[op]

    @staticmethod
    def _parse_u_op(op):
        return {
            'add_rating': submit_rating,
            'add_tag': submit_tag
        }[op]


if __name__ == '__main__':
    ID = 1

    path.append(os.path.dirname(path[0]))
    this_dir = os.path.abspath(__file__)
    os.chdir(os.path.dirname(this_dir))

    from signalhandler import SignalHandler
    from vectorclock import VectorClock
    from statusenum import Status

    stopper = threading.Event()
    rm = ReplicaManager(ID, stopper)
    handler = SignalHandler(stopper, rm)
    signal.signal(signal.SIGINT, handler)
    rm.start()

    with Pyro4.Daemon() as daemon:
        uri = daemon.register(rm)
        with Pyro4.locateNS() as ns:
            ns.register('network.replica.' + ID, uri)

        print('Server ready.')

        daemon.requestLoop(loopCondition=lambda: not stopper.is_set())
