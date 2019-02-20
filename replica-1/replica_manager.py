import Pyro4
import csv
import random
import time


@Pyro4.expose
class MovieRatings(object):
    def __init__(self):
        self.failure_prob = 0.05
        self.overload_prob = 0.2

        self.data_dir = './movielens/'
        self.movies = self.data_dir + 'movies.csv'
        self.ratings = self.data_dir + '/ratings.csv'
        self.tags = self.data_dir + 'tags.csv'

        self.movies_fields = ['movieId', 'title', 'genres']
        self.ratings_fields = ['userId', 'movieId', 'rating', 'timestamp']
        self.tags_fields = ['userId', 'movieId', 'tag', 'timestamp']


    def submit_rating(self, userId, movieId, rating):
        with open(self.ratings, 'a', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, self.ratings_fields)

            writer.writerow({'userId': userId, 'movieId': movieId,
                             'rating': rating, 'timestamp': str(int(time.time()))})


    def submit_tag(self, userId, movieId, tag):
        with open(self.tags, 'a', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, self.tags_fields)

            writer.writerow({'userId': userId, 'movieId': movieId,
                             'tag': tag, 'timestamp': str(int(time.time()))})


    def get_movie_rating(self, movieId):
        ratings = []
        with open(self.ratings, newline='') as csvfile:
            reader = csv.DictReader(csvfile)

            ratings = [float(row['rating']) for row in reader if row['movieId'] == movieId]

        movie_rating = sum(ratings) / len(ratings)
        return movie_rating


    def get_movie_genres(self, movieId):
        movie = []
        with open(self.movies, newline='') as csvfile:
            reader = csv.DictReader(csvfile)

            movie = next(reader, None)
            while movie and movie['movieId'] != movieId:
                movie = next(reader, None)

        genres = movie['genres'].split('|')
        return genres


    def get_movie_tags(self, movieId):
        tags = []
        with open(self.tags, newline='') as csvfile:
            reader = csv.DictReader(csvfile)

            tags = [row['tag'] for row in reader if row['movieId'] == movieId]

        return tags


    def search_by_title(self, title):
        title = title.lower()
        movies = []
        with open(self.movies, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            next(reader, None)  # Skip headers

            movies = [[row['id'], row['title']] for row in reader if title in row['title'].lower()]

        return movies


    def search_by_genre(self, genre):
        genre = genre.lower()
        movies = []
        with open(self.movies, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            next(reader, None)  # Skip headers

            movies = [[row['id'], row['title']] for row in reader if genre in row['genres'].lower()]

        return movies


    def report_status(self):
        failure = random.random()
        overload = random.random()

        if failure < self.failure_prob:
            return 'offline'

        if overload < self.overload_prob:
            return 'over-loaded'

        return 'active'


with Pyro4.Daemon() as daemon:
    uri = daemon.register(MovieRatings)
    with Pyro4.locateNS() as ns:
        ns.register('replica.MovieRatings', uri)

    print('Server ready.')

    daemon.requestLoop()
