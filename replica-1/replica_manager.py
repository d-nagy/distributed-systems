import csv
import queue
import random
import signal
import sys
import threading
import time
import Pyro4


data_dir = './movielens/'
movie_file = data_dir + 'movies.csv'
rating_file = data_dir + '/ratings.csv'
tag_file = data_dir + 'tags.csv'

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


def get_movie_ratings(userId):
    ratings = []
    with open(rating_file, newline='') as csvfile:
        reader = csv.DictReader(csvfile)

        ratings = [dict(row) for row in reader if int(row['userId']) == userId]

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
        self.status = 0

        # Gossip Architecture State
        self.value_ts = [0, 0, 0]
        self.replica_ts = [0, 0, 0]
        self.update_log = []
        self.ts_table = [(0, 0, 0), (0, 0, 0)]
        self.executed = []
        self.pending_queries = queue.Queue()
        self.query_results = {}

        self.stopper = stopper
        self.ts_lock = threading.Lock()
        self.log_lock = threading.Lock()
        self.exec_lock = threading.Lock()
        self.q_res_lock = threading.Lock()

    def run(self):
        while not self.stopper.is_set():
            self.log_lock.acquire()
            updates = self.update_log
            self.log_lock.release()

            # Apply stable updates
            for update in updates:
                _id, ts, u_op, u_prev, u_id = update

                self.ts_lock.acquire()
                stable = self._ts_lte(u_prev, self.value_ts)
                self.ts_lock.release()

                if stable:
                    self._execute_update(u_op, u_id, ts)
                    try:
                        q_op, q_prev = self.pending_queries.get(block=False)

                        self.ts_lock.acquire()
                        self.q_res_lock.acquire()

                        if self._ts_lte(q_prev, self.value_ts):
                            val = self._apply_query(q_op)
                            new = self.value_ts
                            self.query_results[(q_op, q_prev)] = (val, new)

                        self.q_res_lock.release()
                        self.ts_lock.release()
                    except queue.Empty:
                        pass

        print('Stopper set.')

    def send_query(self, q_op, q_prev):
        print('Query received: ', q_op, q_prev)
        val, new = None, None

        self.pending_queries.put((q_op, q_prev))
        self.q_res_lock.acquire()
        self.query_results[(q_op, q_prev)] = None
        self.q_res_lock.release()
        # Wait for query result

        return (val, new)

    def send_update(self, u_op, u_prev, u_id):
        print('Update received: ', u_op, u_prev, u_id)
        ts = None

        self.exec_lock.acquire()
        if u_id not in self.executed:
            self._increment_ts(self.replica_ts)

            ts = u_prev[:]
            ts[self._id] = self.replica_ts[self._id]

            log_record = (self._id, ts, u_op, u_prev, u_id)

            self.log_lock.acquire()
            self.update_log.append(log_record)
            self.log_lock.release()
        self.exec_lock.release()

        return ts

    def _apply_query(self, q_op):
        print('Query applied.')
        val = None

        # Apply query

        return val

    def _apply_update(self, u_op):
        print('Update applied.')
        # Apply update

    def _execute_update(self, u_op, u_id, ts):
        self._apply_update(u_op)

        self.ts_lock.acquire()
        self.value_ts = self._ts_merge(self.value_ts, ts)
        self.ts_lock.release()

        self.exec_lock.acquire()
        self.executed.append(u_id)
        self.exec_lock.release

    def _increment_ts(self, ts):
        ts[self._id] += 1

    @staticmethod
    def _ts_lte(ts_a, ts_b):
        return all([i <= j for i, j in zip(ts_a, ts_b)])

    @staticmethod
    def _ts_merge(ts_a, ts_b):
        return list(map(max, zip(ts_a, ts_b)))


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


if __name__ == '__main__':
    stopper = threading.Event()
    rm = ReplicaManager(1, stopper)
    handler = SignalHandler(stopper, rm)
    signal.signal(signal.SIGINT, handler)
    rm.start()

    with Pyro4.Daemon() as daemon:
        uri = daemon.register(rm)
        with Pyro4.locateNS() as ns:
            ns.register('network.replica', uri)

        print('Server ready.')

        daemon.requestLoop(loopCondition=lambda: not stopper.is_set())
